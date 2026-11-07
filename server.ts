// Newswire. Serve the agents' MCP server and the review page; optionally run the live feed; or replay a file once.
//   node --env-file=.env newswire/server.ts                                  serve /mcp, the review page and the chat (/chat) on localhost (PORT, default 8090)
//   node --env-file=.env newswire/server.ts --feed                           ...and run Alpaca's real-time news through the pipeline
//   node --env-file=.env newswire/server.ts --replay newswire/replay.json    replay once: dedupe -> Jev -> emit, then exit
//     ... --every 10                                                         one headline every 10 s, like a live stream
// Paper trades go to your Alpaca paper account when ALPACA_KEY_ID/ALPACA_SECRET_KEY are set (NEWSWIRE_BROKER=demo keeps the local demo book).
// All three share one SQLite file, so the page and post_view see what the feed and the replay sent.
import { createHmac } from "node:crypto"
import { readFileSync } from "node:fs"
import { parseArgs } from "node:util"
import { Wakeshift } from "@wakeshift/sdk"
import { DEFAULT_THRESHOLD, FEED_SYMBOLS, HOUR, REVIEW_ROUTE, TICKERS, agentKeys, alpacaFeed, alpacaPaper, loadSeed, paper, createApp, createMcp, createPipeline, defineNews, defineReview, funnelLine, jevClient, openDb, owners, routeOf, storyIdOf } from "./app.ts"

const { values } = parseArgs({ options: { replay: { type: "string" }, every: { type: "string" }, feed: { type: "boolean" }, db: { type: "string", default: new URL("./newswire.db", import.meta.url).pathname } } })
const env = process.env
const { WAKESHIFT_KEY, OPENROUTER_API_KEY, JEV_MODEL, NEWSWIRE_AGENT_SECRET, PUBLIC_URL, PORT = "8090", TRUST_PROXY, REVIEWER_TOKEN, ALPACA_KEY_ID, ALPACA_SECRET_KEY } = env
const threshold = Number(env.NEWSWIRE_THRESHOLD ?? DEFAULT_THRESHOLD)
const maxEmitsPerHour = Number(env.NEWSWIRE_MAX_EMITS_PER_HOUR ?? 30)
const problems = [
  !WAKESHIFT_KEY && "WAKESHIFT_KEY is not set",
  (values.replay || values.feed) && !OPENROUTER_API_KEY && 'OPENROUTER_API_KEY is not set (Jev runs through OpenRouter). Add your key (openrouter.ai/settings/keys) with: echo "OPENROUTER_API_KEY=<your key>" >> .env',
  values.feed && !(ALPACA_KEY_ID && ALPACA_SECRET_KEY) && 'The feed needs Alpaca keys (app.alpaca.markets, API keys). Add them with:\n  echo "ALPACA_KEY_ID=<key id>" >> .env\n  echo "ALPACA_SECRET_KEY=<secret>" >> .env',
  !(threshold > 0 && threshold <= 1) && "NEWSWIRE_THRESHOLD must be a number in (0, 1]",
  !(Number.isInteger(maxEmitsPerHour) && maxEmitsPerHour >= 0) && "NEWSWIRE_MAX_EMITS_PER_HOUR must be a whole number",
  PUBLIC_URL && !PUBLIC_URL.startsWith("https://") && "PUBLIC_URL must be this server's https:// address",
].filter(Boolean)
if (problems.length) { console.error(problems.join("\n")); process.exit(1) }

const wake = new Wakeshift({ key: WAKESHIFT_KEY! })
const news = defineNews(wake)
const db = openDb(values.db)
const review = defineReview(wake, owners(db).has)
const pipeline = () => createPipeline({
  db, threshold, maxEmitsPerHour,
  jev: jevClient(OPENROUTER_API_KEY!, JEV_MODEL ? { model: JEV_MODEL } : {}),
  emit: (ticker, payload, id) => news.emit(routeOf(ticker), payload, { id }),
})

if (values.replay) {
  await wake.publishManifest({}) // emit before any agent connects would 404 unknown_event otherwise
  const { ingest, funnel } = pipeline()
  // Replay as fresh news: shift by whole hours so the newest lands in this hour (hour buckets, and so the dupes, stay the same).
  // If this hour's copy was already replayed, step back an hour at a time, so each run sends new stories and wakes agents again.
  const items = JSON.parse(readFileSync(values.replay, "utf8"))
  const at = (i: any, shift: number) => Number.isFinite(Date.parse(i?.publishedAt)) ? new Date(Date.parse(i.publishedAt) + shift).toISOString() : i?.publishedAt
  const newest = Math.max(...items.map((i: any) => Date.parse(i.publishedAt)).filter(Number.isFinite))
  const seen = db.prepare("SELECT 1 FROM stories WHERE story_id = ?")
  let shift = Math.floor(Date.now() / HOUR) * HOUR - Math.floor(newest / HOUR) * HOUR
  while (items.some((i: any) => typeof i?.headline === "string" && Number.isFinite(Date.parse(i.publishedAt)) && seen.get(storyIdOf(i.headline, at(i, shift))))) shift -= HOUR
  const t0 = Date.now()
  const every = Number(values.every ?? 0) * 1000
  for (const [i, item] of items.entries()) { // ponytail: sequential; a pool if replay gets big
    await ingest({ ...item, publishedAt: at(item, shift) })
    if (every) { console.log(`${i + 1}/${items.length} "${String(item?.headline ?? "").slice(0, 70)}" | ${funnelLine(funnel)}`); await new Promise((ok) => setTimeout(ok, every)) }
  }
  console.log(`${funnelLine(funnel)}  (${Date.now() - t0} ms, threshold ${threshold})`)
  process.exit(0)
}

// Agent keys get their own secret. Without one they're derived from WAKESHIFT_KEY and die when it's rotated.
if (!NEWSWIRE_AGENT_SECRET) console.warn('NEWSWIRE_AGENT_SECRET is not set, so agent keys die when WAKESHIFT_KEY is rotated. Set one with: echo "NEWSWIRE_AGENT_SECRET=$(openssl rand -base64 32)" >> .env')
if (!REVIEWER_TOKEN) console.warn('REVIEWER_TOKEN is not set, so nobody can approve or reject. Set one with: echo "REVIEWER_TOKEN=$(openssl rand -hex 24)" >> .env')
const keys = agentKeys(NEWSWIRE_AGENT_SECRET ? Buffer.from(NEWSWIRE_AGENT_SECRET) : createHmac("sha256", WAKESHIFT_KEY!).update("newswire agent keys v1").digest())
// The chat emits before any agent may have connected, so the manifest goes out first (else 404 unknown_event).
await wake.publishManifest({})
// Paper only either way: the Alpaca broker can only reach Alpaca's paper API.
const broker = ALPACA_KEY_ID && ALPACA_SECRET_KEY && env.NEWSWIRE_BROKER !== "demo" ? alpacaPaper({ key: ALPACA_KEY_ID, secret: ALPACA_SECRET_KEY }) : paper(db, loadSeed())
const { server } = createApp({ keys, mcp: createMcp({ wake, news, review, db, keys, broker }), publicUrl: PUBLIC_URL, trustProxy: TRUST_PROXY === "1", db, reviewerToken: REVIEWER_TOKEN, startedAt: Date.now(),
  chat: { broker, emit: (requestId, ask) => review.emit(REVIEW_ROUTE, { requestId, ask }, { id: `chat:${requestId}` }) } })
try {
  const b = await broker.book()
  console.log(b.prices === "alpaca" ? `Paper trades: your Alpaca paper account (value ${b.value.toFixed(2)}, market ${b.marketOpen ? "open" : "closed"}).` : "Paper trades: the local demo book (no Alpaca keys, or NEWSWIRE_BROKER=demo).")
} catch (e: any) { console.warn(`Paper trades: Alpaca isn't answering yet: ${e?.message ?? e}`) }
server.listen(Number(PORT), () => console.log(`newswire on :${PORT}${PUBLIC_URL ? `, public at ${PUBLIC_URL}` : ""}. Chat: http://localhost:${PORT}/chat, review page: http://localhost:${PORT}/ (this machine only). Pool: ${TICKERS.join(", ")}. Threshold ${threshold}, at most ${maxEmitsPerHour} emits an hour`))
if (!db.prepare("SELECT 1 FROM owners LIMIT 1").get()) console.log(`No portfolio owner yet. Get your agent's key with: curl -s -X POST http://localhost:${PORT}/api/owner`)

if (values.feed) {
  const { ingest, funnel } = pipeline()
  // Every story by default, and Jev turns the irrelevant ones away. NEWSWIRE_FEED_SYMBOLS="NVDA,TSM,..." narrows it.
  const symbols = env.NEWSWIRE_FEED_SYMBOLS ? env.NEWSWIRE_FEED_SYMBOLS.split(",").map((s) => s.trim()).filter(Boolean) : FEED_SYMBOLS
  const feed = alpacaFeed({ key: ALPACA_KEY_ID!, secret: ALPACA_SECRET_KEY!, ingest, symbols })
  const hourly = db.prepare("SELECT (SELECT count(*) FROM ingest_log WHERE outcome = 'jev' AND at > ?1) jev, (SELECT count(*) FROM decisions WHERE emitted = 1 AND emitted_at > ?1) emits")
  setInterval(() => {
    const h = hourly.get(Date.now() - HOUR) as any
    console.log(`heartbeat ${new Date().toISOString()} feed ${feed.stats.stopped ? `STOPPED (${feed.stats.stopped})` : feed.stats.connected ? "connected" : "connecting"}, ` +
      `messages in ${feed.stats.messages} (frames ${feed.stats.frames}, skipped ${feed.stats.malformed}, connects ${feed.stats.connects}) | ${funnelLine(funnel)} | last hour: Jev calls ${h.jev}, emits ${h.emits}/${maxEmitsPerHour}`)
  }, 60_000)
}
