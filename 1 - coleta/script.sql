
CREATE SCHEMA "dsp-sauter" AUTHORIZATION postgres;

CREATE TABLE "dsp-sauter".twitchdata (
	channel text NULL,
	watch_time int8 NULL,
	stream_time int8 NULL,
	peak_viewrs int8 NULL,
	average_viewers int8 NULL,
	followers int8 NULL,
	followers_gained int8 NULL,
	views_gained int8 NULL,
	partnered bool NULL,
	mature bool NULL,
	lang text NULL
);
