const axios = require("axios");
const rateLimit = require("function-rate-limit");
const ObjectsToCsv = require("objects-to-csv");
const columnify = require("columnify");

const _ = require("lodash");
const fs = require("fs");
const path = require("path");
const {DateTime} = require("luxon");
const {map} = require("lodash");
const {join} = require("path");

require("dotenv").config();

axios.defaults.headers.common["Authorization"] = process.env.AUTH_BEARER;
axios.defaults.headers.common["Client-ID"] = process.env.CLIENT_ID;

let requests = 0;
let count = 0;
let chunks = 0;
let done = 0;

let start = DateTime.local();
const stop = DateTime.fromISO("2016-01-01T00:00:00+00:00");

let txtFiles = new Map();
let dir = path.join(__dirname, "data", process.env.CHANNEL);

axios
  .get("https://api.twitch.tv/helix/users", {
    params: {
      login: process.env.CHANNEL,
    },
  })
  .then((res) => {
    fs.promises
      .mkdir(dir, {
        recursive: true,
      })
      .then(() => {
        while (start >= stop) {
          const next = start.minus({days: process.env.QUERY_BUCKET_BATCH_DAYS});
          console.log("Starting:", start.toISO(), "-->", next.toISO());
          fetch(res.data.data[0].id, null, start, next);
          chunks++;
          start = next;
        }
      })
      .catch(console.error);
  })
  .catch(console.error);

const fetch = rateLimit(process.env.REQUESTS_PER_SECOND, 1000, function (
  bid,
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
        broadcaster_id: bid,
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

      data = _.chain(res.data.data)
        .filter((d) => d.view_count >= process.env.MINIMUM_VIEWS)
        .map((d) => {
          d["download_url"] = d.thumbnail_url.replace(
            /(-preview-\d+x\d+.jpg)/,
            ".mp4"
          );
          return d;
        })
        .forEach((d) => saveData(d))
        .value();

      console.log(
        columnify(
          [
            {
              Total: "Total: " + count,
              Found: "Found: " + data.length,
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
      count += data.length;

      const nextTs = ts.minus({hours: process.env.QUERY_CLIPS_HOURS});

      if (endTs >= nextTs) {
        console.log("DONE:Time", endTs.toISO(), ">=", nextTs.toISO());
        chunkDone();
        return;
      }

      if (!res.data.pagination.cursor) {
        fetch(bid, null, nextTs, endTs);
      } else {
        fetch(bid, res.data.pagination.cursor, ts, endTs);
      }
    })
    .catch((err) => {
      console.error(
        err.response ? err.response.data : err,
        ts.toISO(),
        curr,
        "RETRYING..."
      );
      fetch(bid, curr, ts, endTs);
    });
});

function chunkDone() {
  done++;
  if (done >= chunks) {
    console.log("ALL DONE!!", count);
    process.exit();
  }
}

async function saveData(clip) {
  const day = DateTime.fromISO(clip.created_at).toFormat("yyyy-LL");

  fs.appendFileSync(
    path.join(dir, `clips_mp4_${process.env.CHANNEL}-${day}.txt`),
    clip.download_url + "\n"
  );

  const csv = new ObjectsToCsv([clip]);

  await csv.toDisk(path.join(dir, `clips_${process.env.CHANNEL}-${day}.csv`), {
    append: true,
  });
}
