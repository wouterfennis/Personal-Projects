"use strict";

function changeVideo(videoId) {
    document.getElementById('video-iframe').src = "https://www.youtube.com/embed/" + videoId;
}