/**
 * Light Novel Audiobook PWA — Main Application Script
 *
 * Handles: navigation, API calls, audio playback, auto-advance,
 * Media Session API, playback position sync, and WebSocket notifications.
 */

// ===================== Configuration =====================

const API_BASE = window.location.origin + "/api";
const WS_URL = `ws://${window.location.host}/ws/notifications`;

// ===================== State =====================

let novels = [];
let currentNovel = null;
let chapters = [];
let currentChapter = null;
let ws = null;

// ===================== DOM Elements =====================

const audio = document.getElementById("audio-player");
const libraryView = document.getElementById("library-view");
const chaptersView = document.getElementById("chapters-view");
const novelList = document.getElementById("novel-list");
const chapterList = document.getElementById("chapter-list");
const chaptersTitle = document.getElementById("chapters-title");
const playerBar = document.getElementById("player-bar");
const requestModal = document.getElementById("request-modal");

// Player controls
const btnPlayPause = document.getElementById("btn-play-pause");
const btnSkipBack = document.getElementById("btn-skip-back");
const btnSkipForward = document.getElementById("btn-skip-forward");
const btnPrevChapter = document.getElementById("btn-prev-chapter");
const btnNextChapter = document.getElementById("btn-next-chapter");
const playerScrubber = document.getElementById("player-scrubber");
const speedControl = document.getElementById("speed-control");
const speedDisplay = document.getElementById("speed-display");
const playerCurrentTime = document.getElementById("player-current-time");
const playerDuration = document.getElementById("player-duration");
const playerNovelTitle = document.getElementById("player-novel-title");
const playerChapterTitle = document.getElementById("player-chapter-title");

// ===================== Navigation =====================

function showView(viewId) {
    document.querySelectorAll(".view").forEach((v) => v.classList.remove("active"));
    document.getElementById(viewId).classList.add("active");
}

document.getElementById("btn-back-library").addEventListener("click", () => {
    showView("library-view");
});

// ===================== Novel Library =====================

async function loadNovels() {
    // TODO: Fetch novels from API
    // novels = await fetch(`${API_BASE}/novels`).then(r => r.json());
    // renderNovelList();
}

function renderNovelList() {
    // TODO: Render novel cards in #novel-list
}

function openNovel(novelId) {
    // TODO: Load chapter list for the selected novel
    // showView("chapters-view");
}

// ===================== Chapter List =====================

async function loadChapters(novelId) {
    // TODO: Fetch chapters from API
    // chapters = await fetch(`${API_BASE}/novels/${novelId}/chapters`).then(r => r.json());
    // renderChapterList();
}

function renderChapterList() {
    // TODO: Render chapter items in #chapter-list
}

// ===================== Audio Playback =====================

function loadChapter(chapter) {
    // TODO: Set audio source and update player UI
    // currentChapter = chapter;
    // audio.src = `${API_BASE}/novels/${currentNovel.id}/chapters/${chapter.chapter_number}/audio`;
    // playerBar.classList.remove("hidden");
}

function formatTime(seconds) {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs.toString().padStart(2, "0")}`;
}

// Play/Pause
btnPlayPause.addEventListener("click", () => {
    if (audio.paused) {
        audio.play();
    } else {
        audio.pause();
    }
});

// Skip forward/back
btnSkipBack.addEventListener("click", () => {
    audio.currentTime = Math.max(0, audio.currentTime - 15);
});

btnSkipForward.addEventListener("click", () => {
    audio.currentTime = Math.min(audio.duration, audio.currentTime + 15);
});

// Playback speed
speedControl.addEventListener("input", () => {
    const speed = parseFloat(speedControl.value);
    audio.playbackRate = speed;
    speedDisplay.textContent = `${speed.toFixed(1)}x`;
});

// Progress scrubber
playerScrubber.addEventListener("input", () => {
    if (audio.duration) {
        audio.currentTime = (playerScrubber.value / 100) * audio.duration;
    }
});

audio.addEventListener("timeupdate", () => {
    if (audio.duration) {
        playerScrubber.value = (audio.currentTime / audio.duration) * 100;
        playerCurrentTime.textContent = formatTime(audio.currentTime);
        playerDuration.textContent = formatTime(audio.duration);
    }
});

// ===================== Auto-Advance =====================

audio.addEventListener("ended", async () => {
    // TODO: Save playback position, then auto-advance to next chapter
    // const nextNum = currentChapter.chapter_number + 1;
    // const nextChapter = chapters.find(c => c.chapter_number === nextNum);
    // if (nextChapter && nextChapter.status === "audio_ready") {
    //     loadChapter(nextChapter);
    //     audio.play();
    // }
});

// ===================== Media Session API =====================

function updateMediaSession() {
    if ("mediaSession" in navigator && currentChapter && currentNovel) {
        navigator.mediaSession.metadata = new MediaMetadata({
            title: currentChapter.title || `Chapter ${currentChapter.chapter_number}`,
            artist: currentNovel.title,
            album: "Light Novel Audiobook",
        });

        navigator.mediaSession.setActionHandler("play", () => audio.play());
        navigator.mediaSession.setActionHandler("pause", () => audio.pause());
        navigator.mediaSession.setActionHandler("previoustrack", () => {
            // TODO: Load previous chapter
        });
        navigator.mediaSession.setActionHandler("nexttrack", () => {
            // TODO: Load next chapter
        });
        navigator.mediaSession.setActionHandler("seekbackward", (details) => {
            audio.currentTime = Math.max(0, audio.currentTime - (details.seekOffset || 15));
        });
        navigator.mediaSession.setActionHandler("seekforward", (details) => {
            audio.currentTime = Math.min(audio.duration, audio.currentTime + (details.seekOffset || 15));
        });
    }
}

// ===================== Playback Position Sync =====================

async function savePlaybackPosition() {
    // TODO: Save position to server
    // await fetch(`${API_BASE}/novels/${currentNovel.id}/playback`, {
    //     method: "PUT",
    //     headers: { "Content-Type": "application/json" },
    //     body: JSON.stringify({
    //         chapter_number: currentChapter.chapter_number,
    //         position_seconds: audio.currentTime,
    //         playback_speed: audio.playbackRate,
    //     }),
    // });
}

async function loadPlaybackPosition(novelId) {
    // TODO: Load saved position from server
    // const state = await fetch(`${API_BASE}/novels/${novelId}/playback`).then(r => r.json());
    // return state;
}

// ===================== WebSocket Notifications =====================

function connectWebSocket() {
    // TODO: Connect to WebSocket for real-time notifications
    // ws = new WebSocket(WS_URL);
    // ws.onmessage = (event) => {
    //     const data = JSON.parse(event.data);
    //     handleNotification(data);
    // };
}

function handleNotification(data) {
    // TODO: Handle chapter_complete, novel_complete, job_progress
}

// ===================== Request Novel =====================

document.getElementById("btn-request-novel").addEventListener("click", () => {
    requestModal.classList.remove("hidden");
});

document.getElementById("btn-cancel-request").addEventListener("click", () => {
    requestModal.classList.add("hidden");
});

document.getElementById("request-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    // TODO: POST to /api/novels with URL and title
    requestModal.classList.add("hidden");
});

// ===================== Service Worker Registration =====================

if ("serviceWorker" in navigator) {
    navigator.serviceWorker
        .register("sw.js")
        .then((reg) => console.log("Service Worker registered:", reg.scope))
        .catch((err) => console.error("Service Worker registration failed:", err));
}

// ===================== Init =====================

async function init() {
    await loadNovels();
    connectWebSocket();
}

init();
