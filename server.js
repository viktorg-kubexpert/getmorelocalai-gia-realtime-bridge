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
const VAD_THRESHOLD = Number(process.env.GIA_VAD_THRESHOLD || 0.75);
const VAD_PREFIX_PADDING_MS = Number(process.env.GIA_VAD_PREFIX_PADDING_MS || 450);
const VAD_SILENCE_DURATION_MS = Number(process.env.GIA_VAD_SILENCE_DURATION_MS || 1000);
const UNCLEAR_REPEAT_LIMIT = Number(process.env.GIA_UNCLEAR_REPEAT_LIMIT || 6);
const UNCLEAR_FALLBACK_COOLDOWN_MS = Number(process.env.GIA_UNCLEAR_FALLBACK_COOLDOWN_MS || 60000);
const UNCLEAR_FORCE_FALLBACK = String(process.env.GIA_UNCLEAR_FORCE_FALLBACK || 'false').toLowerCase() === 'true';
const END_CALL_DELAY_MS = Number(process.env.GIA_END_CALL_DELAY_MS || 6000);
const END_CALL_FALLBACK_DELAY_MS = Number(process.env.GIA_END_CALL_FALLBACK_DELAY_MS || 12000);
const AUDIO_MODE = (process.env.GIA_AUDIO_MODE || '').toLowerCase() || (String(process.env.GIA_ECHO_SUPPRESSION || 'true').toLowerCase() === 'false' ? 'full_duplex' : 'half_duplex');
const ECHO_SUPPRESSION_ENABLED = AUDIO_MODE !== 'full_duplex' && String(process.env.GIA_ECHO_SUPPRESSION || 'true').toLowerCase() !== 'false';
const ECHO_RESUME_DELAY_MS = Number(process.env.GIA_ECHO_RESUME_DELAY_MS || 650);
const ECHO_MARK_DEBOUNCE_MS = Number(process.env.GIA_ECHO_MARK_DEBOUNCE_MS || 120);
const ECHO_MAX_MUTE_MS = Number(process.env.GIA_ECHO_MAX_MUTE_MS || 8000);
const LIVE_TRANSFER_NUMBER = (process.env.LIVE_TRANSFER_NUMBER || '').trim();
const LIVE_TRANSFER_TIMEOUT_SECONDS = Number(process.env.LIVE_TRANSFER_TIMEOUT_SECONDS || 20);
const LIVE_TRANSFER_REQUEST_LIMIT = Number(process.env.LIVE_TRANSFER_REQUEST_LIMIT || 2);

const GIA_INSTRUCTIONS = `You are Gia, GetMoreLocalAI's transparent AI growth assistant on an inbound phone call.

Tone: natural, warm, concise, consultative. You are an AI assistant; do not pretend to be human. Keep most replies to 1-2 short sentences. Ask one question at a time. Let callers interrupt. Be patient in noisy environments; do not rush to answer while the caller is still speaking.

Opening style after Twilio connects: briefly say: "Hi, you reached GetMoreLocalAI. I'm Gia, the AI growth assistant. What can I help you with today?" Do not list every service in the opening.

Business context: GetMoreLocalAI helps small and medium businesses in New York City, North Jersey, Bergen, Hudson, Essex, Passaic, Union, Middlesex, and nearby areas with practical AI automation and local growth systems: AI receptionist, missed-call recovery, website chatbot, lead follow-up, review automation, local SEO / Google Business Profile cleanup, CRM/workflow automation, and Agentic AI Operations Assistant setup.

Main goal: demonstrate value, qualify the caller, and offer a Free AI Growth & Automation Audit or callback.

Collect when natural: caller name, business name, location, website, best callback phone/email, main bottleneck, service interest, urgency/timeframe. Do not interrogate; acknowledge their situation first.

Guardrails: never ask for or accept passwords, API keys, payment cards, private credentials, SSNs, medical/legal sensitive details, or Google/Twilio/WordPress logins. If volunteered, tell them not to share credentials and move on. Do not promise guaranteed leads or fixed pricing; say pricing is scoped after the free audit. If caller asks for a human, live person, customer service, support, billing, or an existing-customer issue: first offer to capture details for follow-up and ask what the call is regarding. If they ask a second time or insist, or if they say urgent, emergency, billing, support issue, existing customer, or customer service, use the transfer_to_live_person tool. If transfer is unavailable, apologize briefly and collect name, callback number/email, issue, and preferred time.

Unclear-audio handling: outdoors, traffic, wind, and speakerphone audio are normal. Do not repeatedly apologize or say the line is noisy. If one utterance is unclear, ask a simple content-based follow-up such as, "Could you repeat that last part?" or "What kind of business is this for?" Only after several failed attempts should you collect a callback number or email so the team can follow up. Do not use the phrase "I may be missing part of what you're saying" unless explicitly instructed by the system. Treat short normal answers like yes, no, okay, bye, thanks, and all set as valid caller input, not unclear audio.

Close: when enough info is collected or caller wants to end, say exactly one brief friendly closing sentence such as, "Thanks for calling GetMoreLocalAI. Have a nice day." Then call the end_call tool. If the caller says goodbye, bye, thanks, that's all, no more questions, or clearly ends the conversation, acknowledge briefly with "Thanks for calling GetMoreLocalAI. Have a nice day." and call the end_call tool. Do not leave the line open after the conversation is finished.`;

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

fastify.get('/healthz', async () => ({ ok: true, service: 'gia-realtime-bridge', realtimeModel: REALTIME_MODEL, audioMode: AUDIO_MODE, echoSuppressionEnabled: ECHO_SUPPRESSION_ENABLED, publicBaseConfigured: Boolean(PUBLIC_BASE_URL), openaiConfigured: Boolean(OPENAI_API_KEY), mailConfigured: Boolean(process.env.SMTP_PASS) }));

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
  <Hangup />
</Response>`;
  return reply.type('text/xml').send(twiml);
});

fastify.get('/twilio/media', { websocket: true }, (twilioSocket, req) => {
  const connId = Math.random().toString(36).slice(2, 10);
  let streamSid = null, callSid = null, caller = 'unknown', openaiSocket = null, openaiReady = false, ended = false, lastResponseId = null, unclearCount = 0, lastUnclearPromptAt = 0;
  let pendingEndCallReason = '', pendingEndCallTimer = null;
  let assistantSpeaking = false, echoIgnoreUntil = 0, pendingEchoMarkTimer = null, echoMarkSeq = 0, lastAssistantAudioAt = 0;
  const transcript = [];
  const startedAt = Date.now();
  const callTimer = setTimeout(() => endCall('max_call_seconds'), MAX_CALL_SECONDS * 1000);
  const log = req.log.child({ connId });
  function pushTranscript(role, text) { const clean = safe(text, 2000); if (!clean) return; transcript.push({ ts: new Date().toISOString(), role, text: clean }); while (transcript.length > MAX_TRANSCRIPT_ITEMS) transcript.shift(); }
  function sendToTwilio(obj) { if (twilioSocket.readyState === WebSocket.OPEN) twilioSocket.send(JSON.stringify(obj)); }
  function sendToOpenAI(obj) { if (openaiSocket && openaiSocket.readyState === WebSocket.OPEN) openaiSocket.send(JSON.stringify(obj)); }
  function echoGuardActive() {
    if (!ECHO_SUPPRESSION_ENABLED) return false;
    const now = Date.now();
    if (assistantSpeaking && now - lastAssistantAudioAt > ECHO_MAX_MUTE_MS) {
      assistantSpeaking = false;
      echoIgnoreUntil = Math.max(echoIgnoreUntil, now + ECHO_RESUME_DELAY_MS);
      log.warn({ maxMuteMs: ECHO_MAX_MUTE_MS }, 'Gia echo guard forced listening resume after max mute window');
    }
    return assistantSpeaking || now < echoIgnoreUntil;
  }
  function scheduleEchoPlaybackMark() {
    if (!ECHO_SUPPRESSION_ENABLED || !streamSid) return;
    if (pendingEchoMarkTimer) clearTimeout(pendingEchoMarkTimer);
    pendingEchoMarkTimer = setTimeout(() => {
      pendingEchoMarkTimer = null;
      if (!streamSid || ended) return;
      const name = `gia_echo_guard_${++echoMarkSeq}`;
      sendToTwilio({ event: 'mark', streamSid, mark: { name } });
      setTimeout(() => {
        if (assistantSpeaking && Date.now() - lastAssistantAudioAt >= ECHO_MARK_DEBOUNCE_MS) {
          assistantSpeaking = false;
          echoIgnoreUntil = Math.max(echoIgnoreUntil, Date.now() + ECHO_RESUME_DELAY_MS);
        }
      }, ECHO_MAX_MUTE_MS).unref?.();
    }, ECHO_MARK_DEBOUNCE_MS);
  }
  function noteAssistantAudioQueued() {
    if (!ECHO_SUPPRESSION_ENABLED) return;
    assistantSpeaking = true;
    lastAssistantAudioAt = Date.now();
    scheduleEchoPlaybackMark();
  }
  function noteAssistantPlaybackComplete(source) {
    if (!ECHO_SUPPRESSION_ENABLED) return;
    assistantSpeaking = false;
    echoIgnoreUntil = Math.max(echoIgnoreUntil, Date.now() + ECHO_RESUME_DELAY_MS);
    log.info({ source, resumeDelayMs: ECHO_RESUME_DELAY_MS }, 'Gia echo guard resumed caller listening after playback grace period');
  }
  function isUnclearTranscript(text) {
    const t = safe(text, 200).toLowerCase();
    if (!t) return true;
    if (/^(yes|yeah|yep|no|nope|ok|okay|sure|right|correct|bye|goodbye|thanks|thank you|all set|nothing else|that's all|that is all)[.!? ]*$/.test(t)) return false;
    if (t.length < 3) return true;
    return /\b(inaudible|unintelligible|background noise|noise|silence|unclear|can't understand|cannot understand)\b/.test(t);
  }
  function handleUnclearAudio(source) {
    unclearCount += 1;
    const now = Date.now();
    log.info({ source, unclearCount }, 'unclear caller audio detected');
    if (UNCLEAR_FORCE_FALLBACK && unclearCount >= UNCLEAR_REPEAT_LIMIT && now - lastUnclearPromptAt > UNCLEAR_FALLBACK_COOLDOWN_MS) {
      lastUnclearPromptAt = now;
      sendToOpenAI({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'System note: Transcription has failed several times. Keep the next reply short and natural. Ask one simple clarifying question, or collect the best callback number/email if a conversation is not workable. Do not say the line is noisy and do not use the phrase "I may be missing part of what you are saying."' }] } });
      sendToOpenAI({ type: 'response.create' });
    }
  }
  function callerWantsToEnd(text) {
    const t = safe(text, 500).toLowerCase();
    return /\b(goodbye|bye|bye bye|that'?s all|that is all|no more questions|nothing else|i'?m all set|all set|thank you,? bye|thanks,? bye|have a good (day|night|one))\b/.test(t);
  }
  function callerWantsLivePerson(text) {
    const t = safe(text, 500).toLowerCase();
    return /\b(live person|real person|human|representative|agent|customer service|support|billing|existing customer|speak to someone|talk to someone|talk to a person|speak with someone|speak with a person|urgent|emergency)\b/.test(t);
  }
  function callerNeedsImmediateTransfer(text) {
    const t = safe(text, 500).toLowerCase();
    return /\b(urgent|emergency|billing|support issue|customer service|existing customer)\b/.test(t);
  }
  async function transferToLivePerson(reason) {
    if (!callSid) throw new Error('missing_call_sid');
    if (!LIVE_TRANSFER_NUMBER) throw new Error('live_transfer_number_not_configured');
    if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN) throw new Error('twilio_rest_credentials_not_configured');
    const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
    const timeout = Math.max(5, Math.min(60, LIVE_TRANSFER_TIMEOUT_SECONDS));
    const fallbackTranscription = 'https://getmorelocalai-phone-2453-gmlai-184328.twil.io/transcription';
    const fallbackRecording = 'https://getmorelocalai-phone-2453-gmlai-184328.twil.io/recording';
    const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="alice">Absolutely. I&apos;ll try to connect you with someone now.</Say>
  <Dial timeout="${timeout}" answerOnBridge="true">${xmlEscape(LIVE_TRANSFER_NUMBER)}</Dial>
  <Say voice="alice">I couldn&apos;t reach someone live. Please leave your name, number, and what you need help with after the tone.</Say>
  <Record maxLength="120" playBeep="true" transcribe="true" transcribeCallback="${fallbackTranscription}" recordingStatusCallback="${fallbackRecording}" />
  <Hangup />
</Response>`;
    await client.calls(callSid).update({ twiml });
    pushTranscript('system', `Live transfer initiated: ${safe(reason, 200)}`);
    void sendCallSummary({ reason: `live_transfer_${safe(reason, 80)}`, callSid, caller, startedAt, transcript }).catch(err => log.error({ err }, 'summary email failed after transfer'));
    ended = true; clearTimeout(callTimer); if (pendingEndCallTimer) clearTimeout(pendingEndCallTimer);
    try { twilioSocket.close(); } catch {}
    try { openaiSocket?.close(); } catch {}
    return true;
  }
  function scheduleEndCall(reason) {
    if (ended) return;
    pendingEndCallReason = reason || 'gia_end_call_tool';
    log.info({ reason: pendingEndCallReason, delayMs: END_CALL_DELAY_MS, fallbackDelayMs: END_CALL_FALLBACK_DELAY_MS }, 'Gia end_call requested; waiting for final response audio before hangup');
    if (pendingEndCallTimer) clearTimeout(pendingEndCallTimer);
    pendingEndCallTimer = setTimeout(() => endCall(pendingEndCallReason), END_CALL_FALLBACK_DELAY_MS);
  }
  function schedulePendingEndCallAfterResponse() {
    if (!pendingEndCallReason || ended) return;
    const reason = pendingEndCallReason;
    pendingEndCallReason = '';
    if (pendingEndCallTimer) { clearTimeout(pendingEndCallTimer); pendingEndCallTimer = null; }
    log.info({ reason, delayMs: END_CALL_DELAY_MS }, 'scheduling Gia hangup after final response completed');
    setTimeout(() => endCall(reason), END_CALL_DELAY_MS);
  }
  function endCall(reason) {
    if (ended) return; ended = true; clearTimeout(callTimer); if (pendingEndCallTimer) clearTimeout(pendingEndCallTimer); log.info({ reason, callSid }, 'ending Gia realtime call');
    try { if (streamSid) sendToTwilio({ event: 'clear', streamSid }); } catch {}
    try { twilioSocket.close(); } catch {}
    try { openaiSocket?.close(); } catch {}
    void sendCallSummary({ reason, callSid, caller, startedAt, transcript }).catch(err => log.error({ err }, 'summary email failed'));
  }
  if (!OPENAI_API_KEY) { log.error('OPENAI_API_KEY missing; cannot start realtime session'); twilioSocket.close(); return; }
  openaiSocket = new WebSocket(`wss://api.openai.com/v1/realtime?model=${encodeURIComponent(REALTIME_MODEL)}`, { headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'OpenAI-Beta': 'realtime=v1' } });
  openaiSocket.on('open', () => {
    openaiReady = true;
    const realtimeTools = [
      { type: 'function', name: 'end_call', description: 'End the phone call after Gia has given a brief closing sentence and the caller is finished.', parameters: { type: 'object', properties: { reason: { type: 'string', description: 'Short reason the call should end, such as caller_goodbye, completed_intake, callback_collected, or conversation_finished.' } }, required: ['reason'] } },
      { type: 'function', name: 'transfer_to_live_person', description: 'Transfer the active Twilio call to a configured live person after the caller insists, asks a second time, or has urgent/support/billing/existing-customer needs. If transfer is unavailable, Gia should collect callback details instead.', parameters: { type: 'object', properties: { reason: { type: 'string', description: 'Why the caller should be transferred, such as second_live_person_request, urgent_support, billing, customer_service, or existing_customer.' } }, required: ['reason'] } }
    ];
    sendToOpenAI({ type: 'session.update', session: { modalities: ['text', 'audio'], instructions: GIA_INSTRUCTIONS, voice: GIA_VOICE, input_audio_format: 'g711_ulaw', output_audio_format: 'g711_ulaw', input_audio_transcription: { model: 'whisper-1' }, turn_detection: { type: 'server_vad', threshold: VAD_THRESHOLD, prefix_padding_ms: VAD_PREFIX_PADDING_MS, silence_duration_ms: VAD_SILENCE_DURATION_MS, create_response: true, interrupt_response: true }, tools: realtimeTools, tool_choice: 'auto', temperature: 0.7, max_response_output_tokens: 700 } });
    sendToOpenAI({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Start the call now with the approved short Gia greeting.' }] } });
    sendToOpenAI({ type: 'response.create' });
  });
  openaiSocket.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    switch (msg.type) {
      case 'response.created': lastResponseId = msg.response?.id || lastResponseId; break;
      case 'response.done': schedulePendingEndCallAfterResponse(); break;
      case 'response.audio.delta': if (msg.delta && streamSid) { noteAssistantAudioQueued(); sendToTwilio({ event: 'media', streamSid, media: { payload: msg.delta } }); } break;
      case 'response.audio_transcript.done': case 'response.output_text.done': pushTranscript('gia', msg.transcript || msg.text || ''); break;
      case 'response.function_call_arguments.done': {
        let args = {}; try { args = JSON.parse(msg.arguments || '{}'); } catch {}
        if (msg.name === 'end_call') {
          scheduleEndCall(args.reason || 'gia_end_call_tool');
        } else if (msg.name === 'transfer_to_live_person') {
          transferToLivePerson(args.reason || 'live_person_requested').catch(err => {
            log.warn({ err: err.message }, 'live transfer unavailable; asking Gia to capture callback');
            sendToOpenAI({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'System note: Live transfer is unavailable right now. Apologize briefly and collect the caller name, callback number or email, issue, and preferred callback time.' }] } });
            sendToOpenAI({ type: 'response.create' });
          });
        }
        break;
      }
      case 'conversation.item.input_audio_transcription.completed': { const text = msg.transcript || ''; if (isUnclearTranscript(text)) handleUnclearAudio('transcription_completed'); else { unclearCount = 0; pushTranscript('caller', text); if (callerWantsToEnd(text)) { sendToOpenAI({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'System note: The caller appears to be ending the conversation. Say exactly: "Thanks for calling GetMoreLocalAI. Have a nice day." Then call the end_call tool.' }] } }); sendToOpenAI({ type: 'response.create' }); } else if (callerWantsLivePerson(text)) { const liveRequests = transcript.filter(t => t.role === 'caller' && callerWantsLivePerson(t.text)).length; if (liveRequests >= LIVE_TRANSFER_REQUEST_LIMIT || callerNeedsImmediateTransfer(text)) { sendToOpenAI({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'System note: The caller has asked for a live person more than once or has an urgent/support/customer-service need. Briefly say you will try to connect them now, then call transfer_to_live_person.' }] } }); sendToOpenAI({ type: 'response.create' }); } } } break; }
      case 'conversation.item.input_audio_transcription.failed': handleUnclearAudio('transcription_failed'); break;
      case 'input_audio_buffer.speech_started': if (!echoGuardActive()) { if (streamSid) sendToTwilio({ event: 'clear', streamSid }); if (lastResponseId) sendToOpenAI({ type: 'response.cancel', response_id: lastResponseId }); } else log.info('Ignored speech_started during Gia echo guard window'); break;
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
      if (openaiReady && msg.media?.payload && !echoGuardActive()) sendToOpenAI({ type: 'input_audio_buffer.append', audio: msg.media.payload });
    } else if (msg.event === 'mark') {
      const name = msg.mark?.name || '';
      if (name.startsWith('gia_echo_guard_')) noteAssistantPlaybackComplete('twilio_mark');
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
