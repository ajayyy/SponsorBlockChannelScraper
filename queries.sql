
-- Highest sponsor durations
select author
, avg(percentageOfVideo) as averagePercentageOfVideo
, avg(duration) as averageSponsorDurationSeconds
, avg(lengthSeconds) / 60 as averageVideoDurationMinutes
, count(distinct videoID) as videoAmount
, sum(duration) / 60 / 60 as totalSponsorHours
, views 

from 
(select videoData.author
, (((MAX(sponsorTimes.endTime - sponsorTimes.startTime))) / count(distinct sponsorTimes.videoID)) as sponsorLengthPerVideo
, ((MAX(sponsorTimes.endTime - sponsorTimes.startTime))) as duration
, ((MAX(sponsorTimes.endTime - sponsorTimes.startTime))) / videoData.lengthSeconds * 100 as percentageOfVideo
, MAX(sponsorTimes.views) as views
, videoData.videoID
, videoData.lengthSeconds

from videoData LEFT JOIN sponsorTimes ON videoData.videoID = sponsorTimes.videoID 

WHERE sponsorTimes.category = "sponsor" AND sponsorTimes.votes >= 0 

AND videoData.lengthSeconds < 2400 -- Less than 50 minutes (short-form content)
AND videoData.lengthSeconds > 60 * 7 -- not really short

GROUP BY videoData.author, videoData.videoID

ORDER BY views DESC)

group by author

HAVING videoAmount > 10 

order by averagePercentageOfVideo desc

-- Sponsor lengths per video
select videoData.title
, ((MAX(sponsorTimes.endTime - sponsorTimes.startTime))) as duration
, sponsorTimes.votes
, videoData.videoID


from videoData LEFT JOIN sponsorTimes ON videoData.videoID = sponsorTimes.videoID 

where videoData.author = "Tom Scott" and sponsorTimes.category = "sponsor" and sponsorTimes.votes >= 0

group by videoData.videoID
order by duration desc