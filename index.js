const axios = require("axios");
const rateLimit = require("function-rate-limit");
const ObjectsToCsv = require("objects-to-csv");
const columnify = require("columnify");

const path = require("path");
const {DateTime} = require("luxon");

require("dotenv").config();

axios.defaults.headers.common["Authorization"] = process.env.AUTH_BEARER;
axios.defaults.headers.common["Client-ID"] = process.env.CLIENT_ID;

let requests = 0;
let count = 0;
let chunk = 0;
let done = 0;

let start = DateTime.local();
const stop = DateTime.fromISO("2016-01-01T00:00:00+00:00");

var fetch = rateLimit(process.env.REQUESTS_PER_SECOND, 1000, function (
  curr,
  ts,
  endTs
) {
  if (typeof curr === "undefined") {
    console.log("done", curr, ts.toISO());
    chunkDone();
    return;
  }
  requests++;
  axios
    .get("https://api.twitch.tv/helix/clips", {
      params: {
        broadcaster_id: process.env.BROADCASTER_ID,
        first: process.env.CLIPS_PER_PAGE,
        after: curr,
        started_at: ts.toISO(),
      },
    })
    .then(async (res) => {
      if (res.data.data.length < 0) {
        console.log("DONE", res.data);
        chunkDone();
        return;
      }

      console.log(
        columnify(
          [
            {
              Total: "Total: " + count,
              Found: "Found: " + res.data.data.length,
              Requests: "Requests: " + requests,
              Time: ts.toISO(),
              Cursor: curr || "N/A",
            },
          ],
          {
            minWidth: 10,
            config: {
              Total: {maxWidth: 16},
              Found: {maxWidth: 16},
            },
            showHeaders: false,
          }
        )
      );
      count += res.data.data.length;

      const csv = new ObjectsToCsv(res.data.data);
      await csv.toDisk(
        path.join(__dirname, `/data/clips_${process.env.FILE_NAME}.csv`),
        {
          append: true,
        }
      );

      const nextTs = ts.minus({hours: process.env.QUERY_CLIPS_HOURS});

      if (endTs >= nextTs) {
        console.log("DONE:Time", endTs.toISO(), ">=", nextTs.toISO());
        chunkDone();
        return;
      }

      if (!res.data.pagination.cursor) {
        fetch(null, nextTs, endTs);
      } else {
        fetch(res.data.pagination.cursor, ts, endTs);
      }
    })
    .catch((err) => {
      console.error(err.response.data, ts.toISO(), curr, "RETRYING...");
      fetch(curr, ts, endTs);
    });
});

function chunkDone() {
  done++;
  if (done >= chunks) {
    console.log("ALL DONE!!", count);
    process.exit();
  }
}

while (start >= stop) {
  const next = start.minus({days: process.env.QUERY_BUCKET_BATCH_DAYS});
  console.log("Starting:", start.toISO(), "-->", next.toISO());
  fetch(null, start, next);
  start = next;
}
