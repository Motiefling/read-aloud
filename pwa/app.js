/**
 * Light Novel Audiobook PWA — Main Application Script
 *
 * Handles: navigation, API calls, audio playback, auto-advance,
 * Media Session API, playback position sync, and WebSocket notifications.
 */

// ===================== Configuration =====================

const API_BASE = window.location.origin + "/api";
const WS_URL = `ws://${window.location.host}/ws/notifications`;
const PLAYBACK_SAVE_INTERVAL = 10000; // save position every 10 seconds

// ===================== State =====================

let novels = [];
let currentNovel = null;
let chapters = [];
let currentChapter = null;
let ws = null;
let playbackSaveTimer = null;

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
    stopJobPolling();
    showView("library-view");
    loadNovels();
});

// ===================== Novel Library =====================

async function loadNovels() {
    try {
        const res = await fetch(`${API_BASE}/novels`);
        if (!res.ok) return;
        novels = await res.json();
        renderNovelList();
    } catch (e) {
        console.error("Failed to load novels:", e);
    }
}

function renderNovelList() {
    novelList.innerHTML = "";
    if (novels.length === 0) {
        novelList.innerHTML =
            '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">' +
            "No novels yet. Tap + to request one.</p>";
        return;
    }
    for (const novel of novels) {
        const card = document.createElement("div");
        card.className = "novel-card";

        const readyCount = novel.processed_chapters || 0;
        const totalCount = novel.total_chapters || 0;
        let statusText = novel.status;
        if (["pending", "processing", "scraped"].includes(novel.status)) {
            statusText = totalCount > 0
                ? `Processing... ${readyCount}/${totalCount} chapters`
                : "Processing...";
        } else if (novel.status === "completed") {
            statusText = `${totalCount} chapters`;
        } else if (novel.status === "failed") {
            statusText = "Failed";
        }

        card.innerHTML =
            `<div class="novel-card-body">` +
            `<div class="novel-title">${escapeHtml(novel.title)}</div>` +
            `<div class="novel-status">${statusText}</div>` +
            `</div>` +
            `<button class="btn-delete-novel" aria-label="Delete novel" title="Delete novel">&times;</button>`;

        card.querySelector(".novel-card-body").addEventListener("click", () => openNovel(novel));
        card.querySelector(".btn-delete-novel").addEventListener("click", (e) => {
            e.stopPropagation();
            deleteNovel(novel);
        });

        novelList.appendChild(card);
    }
}

async function deleteNovel(novel) {
    if (!confirm(`Delete "${novel.title}" and all its audio files?`)) return;
    try {
        const res = await fetch(`${API_BASE}/novels/${novel.id}`, { method: "DELETE" });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to delete novel");
            return;
        }
        // If we were viewing this novel, go back to library
        if (currentNovel && currentNovel.id === novel.id) {
            currentNovel = null;
            showView("library-view");
        }
        await loadNovels();
    } catch (e) {
        console.error("Failed to delete novel:", e);
        alert("Failed to delete novel");
    }
}

let jobPollTimer = null;

async function openNovel(novel) {
    currentNovel = novel;
    chaptersTitle.textContent = novel.title;
    showView("chapters-view");
    await Promise.all([loadChapters(novel.id), loadNovelJobs(novel.id)]);
    startJobPolling();
}

function startJobPolling() {
    stopJobPolling();
    jobPollTimer = setInterval(async () => {
        if (!currentNovel) return;
        await Promise.all([loadNovelJobs(currentNovel.id), loadChapters(currentNovel.id)]);
    }, 3000);
}

function stopJobPolling() {
    if (jobPollTimer) {
        clearInterval(jobPollTimer);
        jobPollTimer = null;
    }
}

// ===================== Check for Updates =====================

document.getElementById("btn-check-updates").addEventListener("click", async () => {
    if (!currentNovel) return;
    try {
        const res = await fetch(`${API_BASE}/novels/${currentNovel.id}/update`, {
            method: "POST",
        });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to check for updates");
            return;
        }
        await loadNovelJobs(currentNovel.id);
    } catch (e) {
        console.error("Failed to check for updates:", e);
        alert("Failed to check for updates");
    }
});

// ===================== Chapter List =====================

async function loadChapters(novelId) {
    try {
        const res = await fetch(`${API_BASE}/novels/${novelId}/chapters`);
        if (!res.ok) return;
        chapters = await res.json();
        renderChapterList();
    } catch (e) {
        console.error("Failed to load chapters:", e);
    }
}

function renderChapterList() {
    chapterList.innerHTML = "";
    if (chapters.length === 0) {
        chapterList.innerHTML =
            '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">' +
            "No chapters yet. Processing may still be running.</p>";
        return;
    }
    for (const ch of chapters) {
        const item = document.createElement("div");
        item.className = "chapter-item";

        const isReady = ch.status === "audio_ready";
        const isPlaying =
            currentChapter && currentChapter.chapter_number === ch.chapter_number;

        let statusLabel = ch.status;
        if (isReady && ch.audio_duration_seconds) {
            statusLabel = formatTime(ch.audio_duration_seconds);
        }

        const chTitle = ch.title && ch.title !== "Untitled"
            ? ch.title
            : `Chapter ${ch.chapter_number}`;
        item.innerHTML =
            `<span class="chapter-number">${ch.chapter_number}</span>` +
            `<span class="chapter-title">${escapeHtml(chTitle)}</span>` +
            `<span class="chapter-status${isPlaying ? " playing" : ""}">${isReady ? statusLabel : ch.status}</span>`;

        if (isReady) {
            item.addEventListener("click", () => loadChapter(ch));
        } else {
            item.style.opacity = "0.5";
        }

        chapterList.appendChild(item);
    }
}

// ===================== Job Status =====================

const jobStatus = document.getElementById("job-status");

async function loadNovelJobs(novelId) {
    if (!jobStatus) return;
    try {
        const res = await fetch(`${API_BASE}/jobs`);
        if (!res.ok) return;
        const allJobs = await res.json();
        const activeJobs = allJobs.filter(
            (j) => j.novel_id === novelId && (j.status === "queued" || j.status === "running"),
        );
        renderJobStatus(activeJobs);
    } catch (e) {
        console.error("Failed to load jobs:", e);
    }
}

function renderJobStatus(jobs) {
    if (!jobStatus) return;
    if (jobs.length === 0) {
        jobStatus.classList.add("hidden");
        return;
    }
    jobStatus.classList.remove("hidden");
    jobStatus.innerHTML = "";
    for (const job of jobs) {
        const item = document.createElement("div");
        item.className = "job-item";
        const pct = Math.round(job.progress_percent || 0);
        const stepText = job.current_step || job.status;
        item.innerHTML =
            `<div class="job-info">` +
            `<span class="job-step">${escapeHtml(stepText)} (${pct}%)</span>` +
            `</div>` +
            `<div class="job-bar"><div class="job-bar-fill" style="width:${pct}%"></div></div>` +
            `<button class="btn-cancel-job" title="Cancel job">Cancel</button>`;
        item.querySelector(".btn-cancel-job").addEventListener("click", () => cancelJob(job.id));
        jobStatus.appendChild(item);
    }
}

async function cancelJob(jobId) {
    if (!confirm("Cancel this job?")) return;
    try {
        const res = await fetch(`${API_BASE}/jobs/${jobId}`, { method: "DELETE" });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to cancel job");
            return;
        }
        if (currentNovel) await loadNovelJobs(currentNovel.id);
    } catch (e) {
        console.error("Failed to cancel job:", e);
    }
}

// ===================== Audio Playback =====================

function loadChapter(chapter) {
    currentChapter = chapter;
    const currentSpeed = parseFloat(speedControl.value);
    audio.src = `${API_BASE}/novels/${currentNovel.id}/chapters/${chapter.chapter_number}/audio`;
    audio.load();
    audio.playbackRate = currentSpeed;

    playerNovelTitle.textContent = currentNovel.title;
    playerChapterTitle.textContent =
        chapter.title && chapter.title !== "Untitled"
            ? chapter.title
            : `Chapter ${chapter.chapter_number}`;
    playerBar.classList.remove("hidden");

    audio.play();
    updateMediaSession();
    renderChapterList(); // refresh to highlight current
    startPlaybackSaving();
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

audio.addEventListener("play", () => {
    btnPlayPause.innerHTML = "&#9646;&#9646;"; // pause icon
});

audio.addEventListener("pause", () => {
    btnPlayPause.innerHTML = "&#9654;"; // play icon
});

// Skip forward/back
btnSkipBack.addEventListener("click", () => {
    audio.currentTime = Math.max(0, audio.currentTime - 15);
});

btnSkipForward.addEventListener("click", () => {
    audio.currentTime = Math.min(audio.duration, audio.currentTime + 15);
});

// Previous/Next chapter
btnPrevChapter.addEventListener("click", () => {
    if (!currentChapter) return;
    const prevNum = currentChapter.chapter_number - 1;
    const prev = chapters.find(
        (c) => c.chapter_number === prevNum && c.status === "audio_ready",
    );
    if (prev) loadChapter(prev);
});

btnNextChapter.addEventListener("click", () => {
    if (!currentChapter) return;
    const nextNum = currentChapter.chapter_number + 1;
    const next = chapters.find(
        (c) => c.chapter_number === nextNum && c.status === "audio_ready",
    );
    if (next) loadChapter(next);
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
    await savePlaybackPosition();
    if (!currentChapter) return;
    const nextNum = currentChapter.chapter_number + 1;
    const nextChapter = chapters.find(
        (c) => c.chapter_number === nextNum && c.status === "audio_ready",
    );
    if (nextChapter) {
        loadChapter(nextChapter);
    }
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
            btnPrevChapter.click();
        });
        navigator.mediaSession.setActionHandler("nexttrack", () => {
            btnNextChapter.click();
        });
        navigator.mediaSession.setActionHandler("seekbackward", (details) => {
            audio.currentTime = Math.max(
                0,
                audio.currentTime - (details.seekOffset || 15),
            );
        });
        navigator.mediaSession.setActionHandler("seekforward", (details) => {
            audio.currentTime = Math.min(
                audio.duration,
                audio.currentTime + (details.seekOffset || 15),
            );
        });
    }
}

// ===================== Playback Position Sync =====================

async function savePlaybackPosition() {
    if (!currentNovel || !currentChapter) return;
    try {
        await fetch(`${API_BASE}/novels/${currentNovel.id}/playback`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                chapter_number: currentChapter.chapter_number,
                position_seconds: audio.currentTime,
                playback_speed: audio.playbackRate,
            }),
        });
    } catch (e) {
        console.error("Failed to save playback position:", e);
    }
}

async function loadPlaybackPosition(novelId) {
    try {
        const res = await fetch(`${API_BASE}/novels/${novelId}/playback`);
        if (!res.ok) return null;
        return await res.json();
    } catch (e) {
        console.error("Failed to load playback position:", e);
        return null;
    }
}

function startPlaybackSaving() {
    stopPlaybackSaving();
    playbackSaveTimer = setInterval(savePlaybackPosition, PLAYBACK_SAVE_INTERVAL);
}

function stopPlaybackSaving() {
    if (playbackSaveTimer) {
        clearInterval(playbackSaveTimer);
        playbackSaveTimer = null;
    }
}

audio.addEventListener("pause", () => {
    savePlaybackPosition();
    stopPlaybackSaving();
});

audio.addEventListener("play", () => {
    startPlaybackSaving();
});

// ===================== WebSocket Notifications =====================

function connectWebSocket() {
    try {
        ws = new WebSocket(WS_URL);
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            handleNotification(data);
        };
        ws.onclose = () => {
            // Reconnect after a short delay
            setTimeout(connectWebSocket, 5000);
        };
        ws.onerror = () => {
            ws.close();
        };
    } catch (e) {
        console.error("WebSocket connection failed:", e);
        setTimeout(connectWebSocket, 5000);
    }
}

function handleNotification(data) {
    if (data.type === "job_progress") {
        loadNovels();
        if (currentNovel) loadNovelJobs(currentNovel.id);
    } else if (data.type === "chapter_complete") {
        if (currentNovel && currentNovel.id === data.novel_id) {
            loadChapters(currentNovel.id);
        }
    } else if (data.type === "novel_complete") {
        loadNovels();
        if (currentNovel && currentNovel.id === data.novel_id) {
            loadChapters(currentNovel.id);
            loadNovelJobs(currentNovel.id);
        }
    }
}

// ===================== Voice Settings =====================

let previewAudio = null;
let previewCache = {}; // voice_id -> Audio element

document.getElementById("btn-settings").addEventListener("click", () => {
    showView("settings-view");
    loadVoices();
});

document.getElementById("btn-back-from-settings").addEventListener("click", () => {
    if (previewAudio) { previewAudio.pause(); }
    showView("library-view");
});

async function loadVoices() {
    const voiceList = document.getElementById("voice-list");
    const currentLabel = document.getElementById("current-voice-label");
    voiceList.innerHTML = '<p style="color: var(--text-secondary);">Loading voices...</p>';

    try {
        const res = await fetch(`${API_BASE}/settings/voices`);
        if (!res.ok) return;
        const data = await res.json();
        currentLabel.textContent = `Current voice: ${data.current_voice}`;
        renderVoiceList(data.voices, data.current_voice);
    } catch (e) {
        console.error("Failed to load voices:", e);
        voiceList.innerHTML = '<p style="color: var(--accent);">Failed to load voices.</p>';
    }
}

function renderVoiceList(voices, currentVoice) {
    const voiceList = document.getElementById("voice-list");
    voiceList.innerHTML = "";

    for (const voice of voices) {
        const card = document.createElement("div");
        card.className = "voice-card" + (voice.id === currentVoice ? " voice-active" : "");

        card.innerHTML =
            `<div class="voice-info">` +
            `<span class="voice-name">${escapeHtml(voice.name)}</span>` +
            `<span class="voice-meta">${voice.accent} ${voice.gender} &middot; Grade ${voice.grade}</span>` +
            `</div>` +
            `<div class="voice-actions">` +
            `<button class="btn-preview-voice" title="Preview">&#9654;</button>` +
            `<button class="btn-select-voice">${voice.id === currentVoice ? "Active" : "Select"}</button>` +
            `</div>`;

        const btnPreview = card.querySelector(".btn-preview-voice");
        btnPreview.addEventListener("click", () => previewVoice(voice.id, btnPreview));

        const btnSelect = card.querySelector(".btn-select-voice");
        if (voice.id !== currentVoice) {
            btnSelect.addEventListener("click", () => selectVoice(voice.id));
        } else {
            btnSelect.disabled = true;
        }

        voiceList.appendChild(card);
    }
}

async function previewVoice(voiceId, btn) {
    // If already playing this voice, pause it
    if (previewAudio && previewAudio._voiceId === voiceId && !previewAudio.paused) {
        previewAudio.pause();
        btn.innerHTML = "&#9654;";
        return;
    }

    // Stop any other preview that's playing
    if (previewAudio && !previewAudio.paused) {
        previewAudio.pause();
        // Reset all preview buttons
        document.querySelectorAll(".btn-preview-voice").forEach(b => { b.innerHTML = "&#9654;"; });
    }

    // Resume if paused midway
    if (previewAudio && previewAudio._voiceId === voiceId && previewAudio.currentTime > 0) {
        previewAudio.play();
        btn.textContent = "||";
        return;
    }

    // Check cache first, otherwise fetch
    if (previewCache[voiceId]) {
        previewAudio = previewCache[voiceId];
        previewAudio.currentTime = 0;
    } else {
        btn.textContent = "...";
        btn.disabled = true;

        try {
            // Fetch the audio as a blob so we can reuse it
            const res = await fetch(`${API_BASE}/settings/voices/${voiceId}/preview`);
            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }
            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            previewAudio = new Audio(url);
            previewAudio._voiceId = voiceId;
            previewCache[voiceId] = previewAudio;
        } catch (e) {
            console.error("Preview failed:", e);
            btn.innerHTML = "&#9654;";
            btn.disabled = false;
            alert("Failed to generate preview");
            return;
        }
    }

    previewAudio._voiceId = voiceId;
    previewAudio.onended = () => {
        btn.innerHTML = "&#9654;";
    };
    previewAudio.onerror = () => {
        btn.innerHTML = "&#9654;";
        btn.disabled = false;
        alert("Failed to play preview");
    };

    try {
        await previewAudio.play();
        btn.textContent = "||";
        btn.disabled = false;
    } catch (e) {
        console.error("Preview play failed:", e);
        btn.innerHTML = "&#9654;";
        btn.disabled = false;
    }
}

async function selectVoice(voiceId) {
    try {
        const res = await fetch(`${API_BASE}/settings/voices/select`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ voice: voiceId }),
        });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to select voice");
            return;
        }
        await loadVoices();
    } catch (e) {
        console.error("Failed to select voice:", e);
        alert("Failed to select voice");
    }
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
    const url = document.getElementById("novel-url").value.trim();
    const title = document.getElementById("novel-title").value.trim();
    const maxChaptersVal = document.getElementById("novel-max-chapters").value.trim();
    const maxChapters = maxChaptersVal ? parseInt(maxChaptersVal) : null;

    if (!url) return;

    try {
        const res = await fetch(`${API_BASE}/novels`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url, title: title || null, max_chapters: maxChapters }),
        });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to submit novel");
            return;
        }
        document.getElementById("novel-url").value = "";
        document.getElementById("novel-title").value = "";
        document.getElementById("novel-max-chapters").value = "";
        requestModal.classList.add("hidden");
        await loadNovels();
    } catch (e) {
        console.error("Failed to request novel:", e);
        alert("Failed to submit novel request");
    }
});

// ===================== Add More Chapters =====================

const addChaptersModal = document.getElementById("add-chapters-modal");

document.getElementById("btn-add-chapters").addEventListener("click", () => {
    addChaptersModal.classList.remove("hidden");
});

document.getElementById("btn-cancel-add-chapters").addEventListener("click", () => {
    addChaptersModal.classList.add("hidden");
});

document.getElementById("add-chapters-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!currentNovel) return;

    const countVal = document.getElementById("add-chapters-count").value.trim();
    const maxChapters = countVal ? parseInt(countVal) : null;

    try {
        const res = await fetch(`${API_BASE}/novels/${currentNovel.id}/add-chapters`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ max_chapters: maxChapters }),
        });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to add chapters");
            return;
        }
        document.getElementById("add-chapters-count").value = "";
        addChaptersModal.classList.add("hidden");
        await loadNovelJobs(currentNovel.id);
    } catch (e) {
        console.error("Failed to add chapters:", e);
        alert("Failed to add chapters");
    }
});

// ===================== Utilities =====================

function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

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
