const axios = require("axios");
const rateLimit = require("function-rate-limit");
const ObjectsToCsv = require("objects-to-csv");
const columnify = require("columnify");

const _ = require("lodash");
const fs = require("fs");
const path = require("path");
const {DateTime} = require("luxon");
const {map, round} = require("lodash");
const {join} = require("path");

require("dotenv").config();

axios.defaults.headers.common["Authorization"] = process.env.AUTH_BEARER;
axios.defaults.headers.common["Client-ID"] = process.env.CLIENT_ID;

let start = DateTime.local();
const stop = DateTime.fromISO("2016-07-26T00:00:00+00:00");

let ids = new Set();

let requests = 0;
let count = 0;
let chunks = 0;
let done = 0;

let pages = 0;

let buckets = Math.floor(
  start.diff(stop).as("hours") / process.env.QUERY_CLIPS_HOURS
);

let bucketsDone = 0;

let directory = "";

const fetch = rateLimit(process.env.REQUESTS_PER_SECOND, 1000, function (
  bid,
  curr,
  ts,
  endTs,
  step
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
        .filter((d) => !ids.has(d.id))
        .forEach((d) => {
          if (ids.has(d.id)) {
            console.log("-----------------------------------------");
            console.log("------------------DUPE-----------------------");
            console.log("----------", d.id, "----------");
            console.log("------------------DUPE-----------------------");
            console.log("-----------------------------------------");
          } else {
            ids.add(d.id);
          }
        })
        .forEach((d) => saveDataCSV(d))
        .filter((d) => d.view_count >= process.env.MINIMUM_VIEWS)
        .forEach((d) => saveDataTXT(d))
        .value();

      console.log(
        columnify(
          [
            {
              Total: "Total: " + count,
              Found: "Found: " + data.length,
              Req: "Req: " + requests,
              Limit: `Rate: ${res.headers["ratelimit-remaining"]}/${res.headers["ratelimit-limit"]}`,
              Time: `${ts.toISO()} ${endTs}`,
              Progress: `Buckets: ${bucketsDone} / ${buckets} (+${pages} pages)`,
              Cursor: curr || "N/A",
            },
          ],
          {
            minWidth: 10,
            config: {
              Total: {minWidth: 16},
              Total: {minWidth: 16},
              Found: {minWidth: 16},
              Req: {minWidth: 12},
              Limit: {minWidth: 16},
              Time: {minWidth: 32},
              Progress: {minWidth: 24},
              Cursor: {minWidth: 16},
            },
            showHeaders: false,
          }
        )
      );
      count += data.length;

      const nextTs = ts.minus(step);

      if (endTs >= nextTs) {
        console.log("DONE:Time", endTs.toISO(), ">=", nextTs.toISO());

        chunkDone();
        return;
      }

      if (!res.data.pagination.cursor) {
        bucketsDone++;
        fetch(bid, null, nextTs, endTs, step);
      } else {
        pages++;
        if (_.endsWith(res.data.pagination.cursor, "9In19")) {
          let nextStep = step;

          if (step.hours && step.hours < 2) {
            nextStep = {minutes: 30};
          } else if (step.minutes && step.minutes >= 15) {
            nextStep = {minutes: round(step.minutes / 2)};
          } else if (step.minutes && step.minutes < 15) {
            console.log(
              "DONE:Time|Page:",
              endTs.toISO(),
              ">=",
              nextTs.toISO(),
              step
            );

            chunkDone();
            return;
          } else if (step.hours) {
            nextStep = {hours: round(step.hours / 2)};
          }

          console.log(
            "---------------------- 10 PAGES DETECTED/START ------------------------"
          );
          console.log("TIME:", ts.toISO(), "FORM:", step, "TO", nextStep);
          console.log(
            "---------------------- 10 PAGES DETECTED/END ------------------------"
          );

          fetch(bid, res.data.pagination.cursor, ts, endTs, nextStep);
        } else {
          fetch(bid, res.data.pagination.cursor, ts, endTs, step);
        }
      }
    })
    .catch((err) => {
      console.error(
        err.response ? err.response.data : err,
        ts.toISO(),
        curr,
        "RETRYING..."
      );
      fetch(bid, curr, ts, endTs, step);
    });
});

function chunkDone() {
  done++;
  if (done >= chunks) {
    console.log("ALL DONE!!", count);
    process.exit();
  }
}

async function saveDataTXT(clip) {
  fs.appendFileSync(
    path.join(directory, `clips_mp4_${process.env.CHANNEL}.txt`),
    clip.download_url + "\n"
  );
}

async function saveDataCSV(clip) {
  const csv = new ObjectsToCsv([clip]);

  await csv.toDisk(path.join(directory, `clips_${process.env.CHANNEL}.csv`), {
    append: true,
  });
}

function fetchChannelInfo() {
  return axios.get("https://api.twitch.tv/helix/users", {
    params: {
      login: process.env.CHANNEL,
    },
  });
}

function createDirectory() {
  const dir = path.join(
    __dirname,
    "data",
    process.env.CHANNEL,
    DateTime.local().toFormat("yyyy-LL-dd--HH_mm_ss")
  );

  return fs.promises
    .mkdir(dir, {
      recursive: true,
    })
    .then(() => {
      directory = dir;
    });
}

function createBuckets(id, start, stop, step) {
  console.log(start.toISO(), stop.toISO(), step);
  return new Promise(async (resolve, reject) => {
    while (start >= stop) {
      const next = start.minus({days: process.env.QUERY_BUCKET_BATCH_DAYS});
      console.log("Starting:", start.toISO(), "-->", next.toISO(), " - ", step);
      fetch(id, null, start, next, step);
      chunks++;
      start = next;
    }
  });
}

createDirectory()
  .then(fetchChannelInfo)
  .then((res) =>
    createBuckets(res.data.data[0].id, start, stop, {
      hours: process.env.QUERY_CLIPS_HOURS,
    })
  )
  .catch(console.error);

// fetchChannelInfo()
//   .then(createDirectory)
//   .then(() =>
//     createBuckets(id, start, stop, {hours: process.env.QUERY_CLIPS_HOURS})
//   )
//   .catch(console.error());
