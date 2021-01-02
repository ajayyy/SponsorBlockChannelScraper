//load database
var Sqlite3 = require('better-sqlite3');
var db = new Sqlite3('./databases/data.db');

const fetch = require("node-fetch");

const config = require("./config.json");
const invidiousURLs = config.invidiousURLs;

// Create table
db.exec(`
CREATE TABLE IF NOT EXISTS "videoData" (
	"videoID"	TEXT UNIQUE,
	"title"	TEXT,
	"maxresdefault_thumbnail"	TEXT,
	"published"	INTEGER,
	"publishedText"	TEXT,
	"viewCount"	INTEGER,
	"likeCount"	INTEGER,
	"author"	TEXT,
	"authorURL"	TEXT,
	"channelThumbnail"	TEXT,
    "lengthSeconds"	INTEGER,
    "category" TEXT
);
`);

const insertQuery = db.prepare("INSERT INTO videoData "
+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

let count = db.prepare("SELECT count(*) as progress from videoData").get().progress;

let videosFetchedInLast30Seconds = 0;
let lastResetTime = Date.now();
let lastSpeed = "N/A";

async function process() {
    let rows = db.prepare(`SELECT sponsorTimes.videoID, MAX(sponsorTimes.views) from sponsorTimes LEFT JOIN videoData ON 
                sponsorTimes.videoID=videoData.videoID WHERE videoData.videoID IS NULL GROUP BY sponsorTimes.videoID ORDER BY sponsorTimes.views DESC`).all();

    let i = 0;

    let runFetchingLoop = async (invidiousURL) => {
        while (i < rows.length) {
            await fetchFromAPI(rows[i++].videoID, invidiousURL);

            videosFetchedInLast30Seconds++;

            const now = Date.now();
            if (now - lastResetTime > 30000) {
                lastResetTime = now;
                lastSpeed = (videosFetchedInLast30Seconds / 30.0).toFixed(2);
                videosFetchedInLast30Seconds = 0;
            }
        }
    };

    for (let instanceIndex = 0; instanceIndex < invidiousURLs.length; instanceIndex++) {
        runFetchingLoop(invidiousURLs[instanceIndex]);
    }
}

[].sort((a, b) => a.width - b.width)

async function fetchFromAPI(videoID, invidiousURL) {
    try {
        const videoDataReq = await fetch(invidiousURL + "/api/v1/videos/" + videoID + "?fields=title,videoThumbnails,published,publishedText,viewCount,likeCount,author,authorUrl,authorThumbnails,lengthSeconds,genre");

        if (videoDataReq.ok) {
            const videoData = await videoDataReq.json();

            insertQuery.run(videoID, videoData.title, videoData.videoThumbnails.find(e => e.quality === "maxresdefault").url
                , videoData.published, videoData.publishedText, videoData.viewCount, videoData.likeCount
                , videoData.author, videoData.authorUrl, videoData.authorThumbnails.sort((a, b) => a.width - b.width)[0].url
                , videoData.lengthSeconds, videoData.genre);

            console.log("added "+ videoID + " (" + videoData.author + ")" + "\t\t" + "Progress: " + ++count + " (" + lastSpeed + " videos/sec)" + "\t\tUsing " + invidiousURL);
        } else {
            console.error("Recieved code " + videoDataReq.status + " for video " + videoID + "\t\tUsing " + invidiousURL)
        }
    } catch (err) {
        console.error(err);
    }
}

process();