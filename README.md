# Newswire

Live market news, filtered by Jev for relevance, wakes your subscribed agents through Wakeshift. They post buy/hold/sell views for a human to review, and you can chat with your always-on agent about your portfolio and make paper trades.

Informational, not financial advice. Nothing here places a real-money trade. With `ALPACA_KEY_ID` and `ALPACA_SECRET_KEY` set, Confirm sends a market order to your Alpaca **paper** account (paper API only, buys capped at cash, no shorting; prices are IEX quotes). Set `NEWSWIRE_BROKER=demo` to use a local SQLite demo book instead.

## Run

Node 22.18+. Put your keys in a `.env` file: `WAKESHIFT_KEY`, `OPENROUTER_API_KEY`, `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY`, `NEWSWIRE_AGENT_SECRET`, `REVIEWER_TOKEN`.

```sh
npm install
npm start                                                       # /mcp, review page (/) and chat (/chat) on :8090
node --env-file=.env server.ts --feed                           # also run Alpaca's real-time news
node --env-file=.env server.ts --replay replay.json --every 10  # replay a fake stream
npm run check                                                   # tests
```

Connect your agent: `curl -s -X POST http://localhost:8090/api/owner`, then run the `claude mcp add …` line it prints.
