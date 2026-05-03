import 'dotenv/config';
import Fastify from 'fastify';
import websocketPlugin from '@fastify/websocket';
import formbodyPlugin from '@fastify/formbody';
import WebSocket from 'ws';
import nodemailer from 'nodemailer';
import twilio from 'twilio';

const fastify = Fastify({ logger: { level: process.env.LOG_LEVEL || 'info' } });
await fastify.register(formbodyPlugin);
await fastify.register(websocketPlugin, { options: { maxPayload: 2 * 1024 * 1024 } });

const PORT = Number(process.env.PORT || 3000);
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
const BRIDGE_TOKEN = process.env.GIA_BRIDGE_TOKEN || '';
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
const REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || 'gpt-4o-realtime-preview';
const SUMMARY_MODEL = process.env.OPENAI_SUMMARY_MODEL || process.env.OPENAI_MODEL || 'gpt-4o-mini';
const GIA_VOICE = process.env.GIA_VOICE || 'alloy';
const MAX_CALL_SECONDS = Number(process.env.GIA_MAX_CALL_SECONDS || 420);
const MAX_TRANSCRIPT_ITEMS = Number(process.env.GIA_MAX_TRANSCRIPT_ITEMS || 80);

const GIA_INSTRUCTIONS = `You are Gia, GetMoreLocalAI's transparent AI growth assistant on an inbound phone call.

Tone: natural, warm, concise, consultative. You are an AI assistant; do not pretend to be human. Keep most replies to 1-2 short sentences. Ask one question at a time. Let callers interrupt.

Opening style after Twilio connects: briefly say: "Hi, you reached GetMoreLocalAI. I'm Gia, the AI growth assistant. What can I help you with today?" Do not list every service in the opening.

Business context: GetMoreLocalAI helps small and medium businesses in New York City, North Jersey, Bergen, Hudson, Essex, Passaic, Union, Middlesex, and nearby areas with practical AI automation and local growth systems: AI receptionist, missed-call recovery, website chatbot, lead follow-up, review automation, local SEO / Google Business Profile cleanup, CRM/workflow automation, and Agentic AI Operations Assistant setup.

Main goal: demonstrate value, qualify the caller, and offer a Free AI Growth & Automation Audit or callback.

Collect when natural: caller name, business name, location, website, best callback phone/email, main bottleneck, service interest, urgency/timeframe. Do not interrogate; acknowledge their situation first.

Guardrails: never ask for or accept passwords, API keys, payment cards, private credentials, SSNs, medical/legal sensitive details, or Google/Twilio/WordPress logins. If volunteered, tell them not to share credentials and move on. Do not promise guaranteed leads or fixed pricing; say pricing is scoped after the free audit. If caller asks for a human, collect callback details and preferred time. If unsure, offer a free audit or callback.

Close: when enough info is collected or caller wants to end, summarize next step and say the team will follow up.`;

function xmlEscape(value) { return String(value || '').replace(/[<>&"']/g, ch => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;' }[ch])); }
function htmlEscape(value) { return String(value || '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch])); }
function safe(value, max = 2000) { return String(value || '').replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, max); }
function publicWsUrl(path) { if (!PUBLIC_BASE_URL) return ''; return PUBLIC_BASE_URL.replace(/^http:/, 'ws:').replace(/^https:/, 'wss:') + path; }
function validateTwilioHttp(req) {
  const authToken = process.env.TWILIO_AUTH_TOKEN || '';
  if (!authToken || String(process.env.SKIP_TWILIO_SIGNATURE_VALIDATION || '').toLowerCase() === 'true') return true;
  const signature = req.headers['x-twilio-signature'];
  const proto = req.headers['x-forwarded-proto'] || req.protocol || 'https';
  const host = req.headers['x-forwarded-host'] || req.headers.host;
  const url = `${proto}://${host}${req.raw.url}`;
  return twilio.validateRequest(authToken, signature, url, req.body || {});
}

fastify.get('/healthz', async () => ({ ok: true, service: 'gia-realtime-bridge', realtimeModel: REALTIME_MODEL, publicBaseConfigured: Boolean(PUBLIC_BASE_URL), openaiConfigured: Boolean(OPENAI_API_KEY), mailConfigured: Boolean(process.env.SMTP_PASS) }));

fastify.all('/twilio/voice', async (req, reply) => {
  if (!validateTwilioHttp(req)) {
    req.log.warn('Rejected /twilio/voice request with invalid Twilio signature');
    return reply.code(403).type('text/plain').send('forbidden');
  }
  const streamUrl = publicWsUrl('/twilio/media');
  if (!streamUrl) return reply.code(500).type('text/xml').send('<?xml version="1.0" encoding="UTF-8"?><Response><Say>Gia is temporarily unavailable. Please try again soon.</Say></Response>');
  const caller = req.body?.From || req.body?.Caller || req.query?.From || 'unknown';
  const callSid = req.body?.CallSid || req.query?.CallSid || '';
  const tokenParam = BRIDGE_TOKEN ? `<Parameter name="bridgeToken" value="${xmlEscape(BRIDGE_TOKEN)}" />` : '';
  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${xmlEscape(streamUrl)}">
      <Parameter name="caller" value="${xmlEscape(caller)}" />
      <Parameter name="callSid" value="${xmlEscape(callSid)}" />
      ${tokenParam}
    </Stream>
  </Connect>
</Response>`;
  return reply.type('text/xml').send(twiml);
});

fastify.get('/twilio/media', { websocket: true }, (twilioSocket, req) => {
  const connId = Math.random().toString(36).slice(2, 10);
  let streamSid = null, callSid = null, caller = 'unknown', openaiSocket = null, openaiReady = false, ended = false, lastResponseId = null;
  const transcript = [];
  const startedAt = Date.now();
  const callTimer = setTimeout(() => endCall('max_call_seconds'), MAX_CALL_SECONDS * 1000);
  const log = req.log.child({ connId });
  function pushTranscript(role, text) { const clean = safe(text, 2000); if (!clean) return; transcript.push({ ts: new Date().toISOString(), role, text: clean }); while (transcript.length > MAX_TRANSCRIPT_ITEMS) transcript.shift(); }
  function sendToTwilio(obj) { if (twilioSocket.readyState === WebSocket.OPEN) twilioSocket.send(JSON.stringify(obj)); }
  function sendToOpenAI(obj) { if (openaiSocket && openaiSocket.readyState === WebSocket.OPEN) openaiSocket.send(JSON.stringify(obj)); }
  function endCall(reason) {
    if (ended) return; ended = true; clearTimeout(callTimer); log.info({ reason, callSid }, 'ending Gia realtime call');
    try { if (streamSid) sendToTwilio({ event: 'clear', streamSid }); } catch {}
    try { twilioSocket.close(); } catch {}
    try { openaiSocket?.close(); } catch {}
    void sendCallSummary({ reason, callSid, caller, startedAt, transcript }).catch(err => log.error({ err }, 'summary email failed'));
  }
  if (!OPENAI_API_KEY) { log.error('OPENAI_API_KEY missing; cannot start realtime session'); twilioSocket.close(); return; }
  openaiSocket = new WebSocket(`wss://api.openai.com/v1/realtime?model=${encodeURIComponent(REALTIME_MODEL)}`, { headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'OpenAI-Beta': 'realtime=v1' } });
  openaiSocket.on('open', () => {
    openaiReady = true;
    sendToOpenAI({ type: 'session.update', session: { modalities: ['text', 'audio'], instructions: GIA_INSTRUCTIONS, voice: GIA_VOICE, input_audio_format: 'g711_ulaw', output_audio_format: 'g711_ulaw', input_audio_transcription: { model: 'whisper-1' }, turn_detection: { type: 'server_vad', threshold: 0.55, prefix_padding_ms: 300, silence_duration_ms: 650, create_response: true, interrupt_response: true }, temperature: 0.7, max_response_output_tokens: 700 } });
    sendToOpenAI({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Start the call now with the approved short Gia greeting.' }] } });
    sendToOpenAI({ type: 'response.create' });
  });
  openaiSocket.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    switch (msg.type) {
      case 'response.created': lastResponseId = msg.response?.id || lastResponseId; break;
      case 'response.audio.delta': if (msg.delta && streamSid) sendToTwilio({ event: 'media', streamSid, media: { payload: msg.delta } }); break;
      case 'response.audio_transcript.done': case 'response.output_text.done': pushTranscript('gia', msg.transcript || msg.text || ''); break;
      case 'conversation.item.input_audio_transcription.completed': pushTranscript('caller', msg.transcript || ''); break;
      case 'input_audio_buffer.speech_started': if (streamSid) sendToTwilio({ event: 'clear', streamSid }); if (lastResponseId) sendToOpenAI({ type: 'response.cancel', response_id: lastResponseId }); break;
      case 'error': log.error({ error: msg.error }, 'OpenAI realtime error'); break;
      default: break;
    }
  });
  openaiSocket.on('error', err => log.error({ err }, 'OpenAI realtime websocket error'));
  openaiSocket.on('close', () => { if (!ended) endCall('openai_closed'); });
  twilioSocket.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.event === 'start') {
      streamSid = msg.start?.streamSid || msg.streamSid || streamSid; callSid = msg.start?.callSid || msg.start?.customParameters?.callSid || callSid; caller = msg.start?.customParameters?.caller || caller;
      const suppliedToken = msg.start?.customParameters?.bridgeToken || '';
      if (BRIDGE_TOKEN && suppliedToken !== BRIDGE_TOKEN) { log.warn('Rejected media stream: invalid bridge token'); endCall('invalid_bridge_token'); } else log.info({ streamSid, callSid, caller }, 'Twilio media stream started');
    } else if (msg.event === 'media') {
      if (openaiReady && msg.media?.payload) sendToOpenAI({ type: 'input_audio_buffer.append', audio: msg.media.payload });
    } else if (msg.event === 'stop') endCall('twilio_stop');
  });
  twilioSocket.on('close', () => { if (!ended) endCall('twilio_closed'); });
  twilioSocket.on('error', err => log.error({ err }, 'Twilio websocket error'));
});

async function summarizeLead({ reason, callSid, caller, startedAt, transcript }) {
  const transcriptText = transcript.map(t => `[${t.role}] ${t.text}`).join('\n');
  if (!OPENAI_API_KEY || !transcriptText) return { summary: transcriptText || '(No transcript captured.)', fields: {} };
  const prompt = `Summarize this GetMoreLocalAI Gia phone call for sales follow-up. Return JSON with keys: lead_summary, caller_name, business_name, location, website, phone, email, service_interest, pain_point, urgency, recommended_next_action. Do not invent missing fields; use empty strings.\n\nCaller number: ${caller}\nCall SID: ${callSid}\nEnd reason: ${reason}\nTranscript:\n${transcriptText.slice(0, 12000)}`;
  const resp = await fetch('https://api.openai.com/v1/chat/completions', { method: 'POST', headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ model: SUMMARY_MODEL, temperature: 0.1, max_tokens: 600, response_format: { type: 'json_object' }, messages: [{ role: 'user', content: prompt }] }) });
  if (!resp.ok) throw new Error(`OpenAI summary failed ${resp.status}`);
  const data = await resp.json();
  const parsed = JSON.parse(data.choices?.[0]?.message?.content || '{}');
  return { summary: parsed.lead_summary || transcriptText, fields: parsed };
}
async function sendCallSummary({ reason, callSid, caller, startedAt, transcript }) {
  if (String(process.env.SEND_SUMMARY_EMAIL || 'true').toLowerCase() !== 'true') return false;
  if (!process.env.SMTP_PASS) { fastify.log.warn('SMTP_PASS missing; skipping Gia summary email'); return false; }
  const { summary, fields } = await summarizeLead({ reason, callSid, caller, startedAt, transcript }).catch(err => { fastify.log.error({ err }, 'lead summary generation failed'); return { summary: transcript.map(t => `[${t.role}] ${t.text}`).join('\n') || '(No transcript captured.)', fields: {} }; });
  const durationSec = Math.round((Date.now() - startedAt) / 1000);
  const rows = [['Caller number', caller], ['Call SID', callSid], ['Duration', `${durationSec}s`], ['End reason', reason], ['Name', fields.caller_name], ['Business', fields.business_name], ['Location', fields.location], ['Website', fields.website], ['Phone', fields.phone], ['Email', fields.email], ['Service interest', fields.service_interest], ['Pain point', fields.pain_point], ['Urgency', fields.urgency], ['Recommended next action', fields.recommended_next_action || 'Follow up and offer the Free AI Growth & Automation Audit.']].filter(([, v]) => v);
  const transcriptText = transcript.map(t => `${t.role}: ${t.text}`).join('\n');
  const text = ['New GetMoreLocalAI Gia realtime call.', '', ...rows.map(([k, v]) => `${k}: ${v}`), '', 'Lead summary:', summary, '', 'Transcript:', transcriptText || '(No transcript captured.)'].join('\n');
  const htmlRows = rows.map(([k, v]) => `<tr><th style="text-align:left;padding:6px 12px 6px 0;color:#334155;white-space:nowrap;">${htmlEscape(k)}</th><td style="padding:6px 0;color:#0f172a;">${htmlEscape(v)}</td></tr>`).join('');
  const html = `<div style="font-family:Arial,Helvetica,sans-serif;max-width:760px;margin:auto"><h2>New GetMoreLocalAI Gia realtime call</h2><table>${htmlRows}</table><h3>Lead summary</h3><p>${htmlEscape(summary)}</p><h3>Transcript</h3><pre style="white-space:pre-wrap;background:#f6f8fa;padding:12px;border-radius:8px">${htmlEscape(transcriptText || '(No transcript captured.)')}</pre></div>`;
  const transporter = nodemailer.createTransport({ host: process.env.SMTP_HOST || 'gator4126.hostgator.com', port: Number(process.env.SMTP_PORT || 465), secure: true, auth: { user: process.env.SMTP_USER || 'agent@getmorelocalai.com', pass: process.env.SMTP_PASS } });
  await transporter.sendMail({ from: process.env.MAIL_FROM || process.env.SMTP_USER || 'agent@getmorelocalai.com', to: process.env.MAIL_TO || 'info@getmorelocalai.com', subject: `New GetMoreLocalAI Gia call${fields.business_name ? ` — ${fields.business_name}` : caller ? ` from ${caller}` : ''}`, text, html });
  fastify.log.info({ callSid, caller }, 'Gia summary email sent');
  return true;
}

fastify.listen({ port: PORT, host: '0.0.0.0' }).catch(err => { fastify.log.error(err); process.exit(1); });
