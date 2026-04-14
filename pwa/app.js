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
let listenedChapters = loadListenedChapters(); // { novelId: [chapterNumbers] }

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
        const [novelsRes, jobsRes] = await Promise.all([
            fetch(`${API_BASE}/novels`),
            fetch(`${API_BASE}/jobs`),
        ]);
        if (!novelsRes.ok) return;
        novels = await novelsRes.json();

        // Tag novels that have interrupted jobs
        if (jobsRes.ok) {
            const allJobs = await jobsRes.json();
            const interruptedNovelIds = new Set(
                allJobs.filter((j) => j.status === "interrupted").map((j) => j.novel_id),
            );
            for (const novel of novels) {
                novel._hasInterruptedJob = interruptedNovelIds.has(novel.id);
            }
        }

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

        // Check if this novel has interrupted jobs (from server restart)
        if (novel._hasInterruptedJob) {
            statusText += " — interrupted, tap to resume";
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
    const titleLink = document.getElementById("chapters-title-link");
    titleLink.textContent = novel.title;
    titleLink.href = novel.source_url || "#";
    showView("chapters-view");
    await Promise.all([loadChapters(novel.id), loadNovelJobs(novel.id)]);
}

function startJobPolling() {
    stopJobPolling();
    jobPollTimer = setInterval(async () => {
        if (!currentNovel) return;
        await loadNovelJobs(currentNovel.id);
        // loadNovelJobs will call stopJobPolling() if no active jobs remain,
        // so only refresh chapters if we're still polling
        if (jobPollTimer) {
            await loadChapters(currentNovel.id);
            // Re-fetch novel metadata so title updates appear promptly
            await refreshNovelMetadata(currentNovel.id);
        }
    }, 10000);
}

async function refreshNovelMetadata(novelId) {
    try {
        const res = await fetch(`${API_BASE}/novels/${novelId}`);
        if (!res.ok) return;
        const novel = await res.json();
        if (novel.title && novel.title !== currentNovel.title) {
            currentNovel.title = novel.title;
            const titleLink = document.getElementById("chapters-title-link");
            if (titleLink) titleLink.textContent = novel.title;
        }
    } catch (e) {
        console.error("Failed to refresh novel metadata:", e);
    }
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
    const btn = document.getElementById("btn-check-updates");
    btn.disabled = true;
    btn.textContent = "...";
    try {
        const res = await fetch(`${API_BASE}/novels/${currentNovel.id}/check-updates`, {
            method: "POST",
        });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to check for updates");
            btn.disabled = false;
            btn.innerHTML = "&#x21bb;";
            return;
        }
        const data = await res.json();
        await loadNovelJobs(currentNovel.id);
        pollCheckJob(data.job_id);
    } catch (e) {
        console.error("Failed to check for updates:", e);
        alert("Failed to check for updates");
        btn.disabled = false;
        btn.innerHTML = "&#x21bb;";
    }
});

async function pollCheckJob(jobId) {
    const btn = document.getElementById("btn-check-updates");
    const poll = setInterval(async () => {
        try {
            const res = await fetch(`${API_BASE}/jobs/${jobId}`);
            if (!res.ok) { clearInterval(poll); btn.disabled = false; btn.innerHTML = "&#x21bb;"; return; }
            const job = await res.json();

            if (job.status === "completed") {
                clearInterval(poll);
                btn.disabled = false;
                btn.innerHTML = "&#x21bb;";
                const match = job.current_step && job.current_step.match(/Found (\d+) new chapters/);
                const count = match ? parseInt(match[1]) : 0;
                if (count > 0) {
                    showProcessConfirmation(count);
                } else {
                    alert("No new chapters found.");
                }
                if (currentNovel) {
                    await loadNovelJobs(currentNovel.id);
                    await loadChapters(currentNovel.id);
                }
            } else if (job.status === "failed" || job.status === "cancelled") {
                clearInterval(poll);
                btn.disabled = false;
                btn.innerHTML = "&#x21bb;";
                if (job.status === "failed") {
                    alert("Check failed: " + (job.error_message || "Unknown error"));
                }
                if (currentNovel) await loadNovelJobs(currentNovel.id);
            }
        } catch (e) {
            clearInterval(poll);
            btn.disabled = false;
            btn.innerHTML = "&#x21bb;";
            console.error("Poll failed:", e);
        }
    }, 2000);
}

function showProcessConfirmation(count) {
    const heading = document.querySelector("#add-chapters-modal h2");
    const label = document.querySelector("#add-chapters-modal label[for='add-chapters-count']");
    const input = document.getElementById("add-chapters-count");
    heading.textContent = `${count} New Chapters Available`;
    label.textContent = "How many to process?";
    input.placeholder = `Up to ${count}`;
    input.value = "";
    document.getElementById("add-chapters-start-url").value = "";
    addChaptersModal.classList.remove("hidden");
}

// ===================== Novel Rename =====================

document.getElementById("btn-rename-novel").addEventListener("click", async () => {
    if (!currentNovel) return;
    const newTitle = prompt("Rename novel:", currentNovel.title);
    if (newTitle === null || newTitle.trim() === "" || newTitle.trim() === currentNovel.title) return;
    try {
        const res = await fetch(`${API_BASE}/novels/${currentNovel.id}`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ title: newTitle.trim() }),
        });
        if (res.ok) {
            currentNovel.title = newTitle.trim();
            const titleLink = document.getElementById("chapters-title-link");
            titleLink.textContent = newTitle.trim();
        } else {
            alert("Failed to rename novel");
        }
    } catch (e) {
        console.error("Failed to rename novel:", e);
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
        const isReady = ch.status === "audio_ready";
        const isError = ch.status === "error";
        const isPlaying =
            currentChapter && currentChapter.chapter_number === ch.chapter_number;
        const listened = currentNovel && isChapterListened(currentNovel.id, ch.chapter_number);

        const item = document.createElement("div");
        item.className = "chapter-item" + (listened ? " chapter-listened" : "");

        let statusLabel = ch.status;
        if (isReady && ch.audio_duration_seconds) {
            statusLabel = formatTime(ch.audio_duration_seconds);
        }

        const chTitle = ch.title_english
            ? ch.title_english
            : ch.title && ch.title !== "Untitled"
                ? ch.title
                : `Chapter ${ch.chapter_number}`;

        let actionsHtml = "";
        if (isError) {
            actionsHtml =
                `<button class="btn-ch-retry" title="Retry">&#x21bb;</button>` +
                `<button class="btn-ch-delete" title="Delete">&times;</button>`;
        } else {
            actionsHtml =
                `<button class="btn-ch-rename" title="Rename">&#9998;</button>` +
                `<button class="btn-ch-delete" title="Delete">&times;</button>`;
        }

        const listenedIcon = listened ? '<span class="chapter-listened-icon" title="Listened">&#10003;</span>' : "";

        item.innerHTML =
            `<span class="chapter-number">${ch.chapter_number}</span>` +
            `<span class="chapter-title">${escapeHtml(chTitle)}</span>` +
            `${listenedIcon}` +
            `<span class="chapter-status${isPlaying ? " playing" : ""}">${isReady ? statusLabel : ch.status}</span>` +
            `<span class="chapter-actions">${actionsHtml}</span>`;

        // Rename button
        const btnRename = item.querySelector(".btn-ch-rename");
        if (btnRename) {
            btnRename.addEventListener("click", async (e) => {
                e.stopPropagation();
                const newTitle = prompt("Rename chapter:", ch.title || `Chapter ${ch.chapter_number}`);
                if (newTitle === null || newTitle.trim() === "") return;
                try {
                    const res = await fetch(
                        `${API_BASE}/novels/${currentNovel.id}/chapters/${ch.chapter_number}`,
                        { method: "PATCH", headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ title: newTitle.trim() }) },
                    );
                    if (res.ok) { ch.title = newTitle.trim(); renderChapterList(); }
                } catch (err) { console.error("Failed to rename chapter:", err); }
            });
        }

        // Delete button
        const btnDelete = item.querySelector(".btn-ch-delete");
        if (btnDelete) {
            btnDelete.addEventListener("click", async (e) => {
                e.stopPropagation();
                if (!confirm(`Delete chapter ${ch.chapter_number}?`)) return;
                try {
                    const res = await fetch(
                        `${API_BASE}/novels/${currentNovel.id}/chapters/${ch.chapter_number}`,
                        { method: "DELETE" },
                    );
                    if (res.ok) { await loadChapters(currentNovel.id); }
                    else { alert("Failed to delete chapter"); }
                } catch (err) { console.error("Failed to delete chapter:", err); }
            });
        }

        // Retry button (error chapters)
        const btnRetry = item.querySelector(".btn-ch-retry");
        if (btnRetry) {
            btnRetry.addEventListener("click", async (e) => {
                e.stopPropagation();
                try {
                    const res = await fetch(
                        `${API_BASE}/novels/${currentNovel.id}/chapters/${ch.chapter_number}/retry`,
                        { method: "POST" },
                    );
                    if (res.ok) {
                        const data = await res.json();
                        await loadNovelJobs(currentNovel.id);
                        startJobPolling();
                        await loadChapters(currentNovel.id);
                    } else { alert("Failed to retry chapter"); }
                } catch (err) { console.error("Failed to retry chapter:", err); }
            });
        }

        // Click row to play (audio_ready chapters only)
        if (isReady) {
            item.addEventListener("click", () => loadChapter(ch));
        } else if (!isError) {
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
            (j) => j.novel_id === novelId &&
                   (j.status === "queued" || j.status === "running" || j.status === "interrupted"),
        );
        renderJobStatus(activeJobs);

        // Start or stop polling based on whether there are active jobs
        const hasRunning = activeJobs.some((j) => j.status === "queued" || j.status === "running");
        if (hasRunning && !jobPollTimer) {
            startJobPolling();
        } else if (!hasRunning) {
            stopJobPolling();
        }
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

        if (job.status === "interrupted") {
            item.innerHTML =
                `<div class="job-info">` +
                `<span class="job-step job-interrupted">${escapeHtml(stepText)}</span>` +
                `</div>` +
                `<div class="job-actions-row">` +
                `<button class="btn-resume-job" title="Resume job">Resume</button>` +
                `<button class="btn-dismiss-job" title="Dismiss">Dismiss</button>` +
                `</div>`;
            item.querySelector(".btn-resume-job").addEventListener("click", () => retryJob(job.id));
            item.querySelector(".btn-dismiss-job").addEventListener("click", () => dismissJob(job.id));
        } else {
            item.innerHTML =
                `<div class="job-info">` +
                `<span class="job-step">${escapeHtml(stepText)} (${pct}%)</span>` +
                `</div>` +
                `<div class="job-bar"><div class="job-bar-fill" style="width:${pct}%"></div></div>` +
                `<button class="btn-cancel-job" title="Cancel job">Cancel</button>`;
            item.querySelector(".btn-cancel-job").addEventListener("click", () => cancelJob(job.id));
        }
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
        if (currentNovel) {
            await loadNovelJobs(currentNovel.id);
            await loadChapters(currentNovel.id);
        }
    } catch (e) {
        console.error("Failed to cancel job:", e);
    }
}

async function retryJob(jobId) {
    try {
        const res = await fetch(`${API_BASE}/jobs/${jobId}/retry`, { method: "POST" });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to resume job");
            return;
        }
        if (currentNovel) {
            await loadNovelJobs(currentNovel.id);
            await loadChapters(currentNovel.id);
            startJobPolling();
        }
    } catch (e) {
        console.error("Failed to retry job:", e);
        alert("Failed to resume job");
    }
}

async function dismissJob(jobId) {
    try {
        const res = await fetch(`${API_BASE}/jobs/${jobId}`, { method: "DELETE" });
        if (!res.ok) {
            // If already completed/cancelled, just reload
            if (res.status !== 400) {
                const err = await res.json();
                alert(err.detail || "Failed to dismiss job");
                return;
            }
        }
        if (currentNovel) {
            await loadNovelJobs(currentNovel.id);
            await loadChapters(currentNovel.id);
        }
    } catch (e) {
        console.error("Failed to dismiss job:", e);
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
    playerChapterTitle.textContent = chapter.title_english
        ? chapter.title_english
        : chapter.title && chapter.title !== "Untitled"
            ? chapter.title
            : `Chapter ${chapter.chapter_number}`;
    playerBar.classList.remove("hidden");

    audio.play();
    updateMediaSession();
    renderChapterList(); // refresh to highlight current
    startPlaybackSaving();
    savePlaybackPositionLocal();
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
    if ("mediaSession" in navigator) navigator.mediaSession.playbackState = "playing";
});

audio.addEventListener("pause", () => {
    btnPlayPause.innerHTML = "&#9654;"; // play icon
    if ("mediaSession" in navigator) navigator.mediaSession.playbackState = "paused";
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
    updatePositionState();
});

// Progress scrubber
playerScrubber.addEventListener("input", () => {
    if (audio.duration) {
        audio.currentTime = (playerScrubber.value / 100) * audio.duration;
    }
});

audio.addEventListener("loadedmetadata", () => {
    updatePositionState();
});

audio.addEventListener("timeupdate", () => {
    if (audio.duration) {
        playerScrubber.value = (audio.currentTime / audio.duration) * 100;
        playerCurrentTime.textContent = formatTime(audio.currentTime);
        playerDuration.textContent = formatTime(audio.duration);
        updatePositionState();
        // Mark as listened once past 90%
        if (currentNovel && currentChapter && audio.currentTime / audio.duration > 0.9) {
            markChapterListened(currentNovel.id, currentChapter.chapter_number);
        }
    }
});

// ===================== Auto-Advance =====================

audio.addEventListener("ended", async () => {
    if (currentNovel && currentChapter) {
        markChapterListened(currentNovel.id, currentChapter.chapter_number);
        renderChapterList();
    }
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
    if (!("mediaSession" in navigator) || !currentChapter || !currentNovel) return;

    const artwork = currentNovel.cover_image_path
        ? [{ src: `${API_BASE}/novels/${currentNovel.id}/cover`, sizes: "512x512", type: "image/jpeg" }]
        : [
            { src: "icons/icon-192.png", sizes: "192x192", type: "image/png" },
            { src: "icons/icon-512.png", sizes: "512x512", type: "image/png" },
        ];

    navigator.mediaSession.metadata = new MediaMetadata({
        title: currentChapter.title_english || currentChapter.title || `Chapter ${currentChapter.chapter_number}`,
        artist: currentNovel.title,
        album: "Light Novel Audiobook",
        artwork,
    });

    navigator.mediaSession.setActionHandler("play", () => audio.play());
    navigator.mediaSession.setActionHandler("pause", () => audio.pause());
    navigator.mediaSession.setActionHandler("stop", () => {
        audio.pause();
        audio.currentTime = 0;
    });

    // Use previoustrack/nexttrack for 15-second seek since those buttons
    // reliably show in Android notifications. Chapter navigation is handled
    // by auto-advance and the in-app UI.
    navigator.mediaSession.setActionHandler("previoustrack", () => {
        audio.currentTime = Math.max(0, audio.currentTime - 15);
        updatePositionState();
    });
    navigator.mediaSession.setActionHandler("nexttrack", () => {
        audio.currentTime = Math.min(audio.duration, audio.currentTime + 15);
        updatePositionState();
    });
    navigator.mediaSession.setActionHandler("seekbackward", (details) => {
        audio.currentTime = Math.max(
            0,
            audio.currentTime - (details.seekOffset || 15),
        );
        updatePositionState();
    });
    navigator.mediaSession.setActionHandler("seekforward", (details) => {
        audio.currentTime = Math.min(
            audio.duration,
            audio.currentTime + (details.seekOffset || 15),
        );
        updatePositionState();
    });
    navigator.mediaSession.setActionHandler("seekto", (details) => {
        if (details.fastSeek && "fastSeek" in audio) {
            audio.fastSeek(details.seekTime);
        } else {
            audio.currentTime = details.seekTime;
        }
        updatePositionState();
    });
}

function updatePositionState() {
    if (!("mediaSession" in navigator) || !audio.duration || !isFinite(audio.duration)) return;
    navigator.mediaSession.setPositionState({
        duration: audio.duration,
        playbackRate: audio.playbackRate,
        position: audio.currentTime,
    });
}

// ===================== Playback Position Sync =====================

function savePlaybackPositionLocal() {
    if (!currentNovel || !currentChapter) return;
    const state = {
        novel_id: currentNovel.id,
        novel_title: currentNovel.title,
        chapter_number: currentChapter.chapter_number,
        position_seconds: audio.currentTime,
        playback_speed: audio.playbackRate,
    };
    localStorage.setItem("playback_state", JSON.stringify(state));
    localStorage.setItem("last_novel_id", String(currentNovel.id));
}

async function savePlaybackPosition() {
    if (!currentNovel || !currentChapter) return;
    savePlaybackPositionLocal();
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

function loadPlaybackPositionLocal() {
    try {
        const data = JSON.parse(localStorage.getItem("playback_state") || "null");
        return data;
    } catch {
        return null;
    }
}

async function loadPlaybackPosition(novelId) {
    // Try server first, fall back to localStorage
    try {
        const res = await fetch(`${API_BASE}/novels/${novelId}/playback`);
        if (res.ok) {
            const data = await res.json();
            // Server returns default chapter_number=1, position=0 when no state saved
            // Check if it's a real saved state (position > 0 or chapter > 1)
            if (data.position_seconds > 0 || data.chapter_number > 1) {
                return data;
            }
        }
    } catch (e) {
        console.error("Failed to load playback position from server:", e);
    }
    // Fall back to localStorage
    const local = loadPlaybackPositionLocal();
    if (local && String(local.novel_id) === String(novelId)) {
        return local;
    }
    return null;
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

// Save position when page is closed or hidden (e.g. switching tabs, closing app)
window.addEventListener("beforeunload", () => {
    savePlaybackPositionLocal();
});

document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
        savePlaybackPositionLocal();
    }
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
    const startChapterUrl = document.getElementById("novel-start-url").value.trim();
    const maxChaptersVal = document.getElementById("novel-max-chapters").value.trim();
    const maxChapters = maxChaptersVal ? parseInt(maxChaptersVal) : null;

    if (!url) return;

    try {
        const res = await fetch(`${API_BASE}/novels`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                url,
                title: title || null,
                max_chapters: maxChapters,
                start_chapter_url: startChapterUrl || null,
            }),
        });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to submit novel");
            return;
        }
        document.getElementById("novel-url").value = "";
        document.getElementById("novel-title").value = "";
        document.getElementById("novel-start-url").value = "";
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
    // Reset modal to default state (may have been modified by check-updates flow)
    document.querySelector("#add-chapters-modal h2").textContent = "Download More Chapters";
    document.querySelector("#add-chapters-modal label[for='add-chapters-count']").textContent = "How many more chapters?";
    document.getElementById("add-chapters-start-url").value = "";
    document.getElementById("add-chapters-count").placeholder = "All remaining";
    addChaptersModal.classList.remove("hidden");
});

document.getElementById("btn-cancel-add-chapters").addEventListener("click", () => {
    addChaptersModal.classList.add("hidden");
});

document.getElementById("add-chapters-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!currentNovel) return;

    const startUrl = document.getElementById("add-chapters-start-url").value.trim();
    const countVal = document.getElementById("add-chapters-count").value.trim();
    const maxChapters = countVal ? parseInt(countVal) : null;

    try {
        const res = await fetch(`${API_BASE}/novels/${currentNovel.id}/add-chapters`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                max_chapters: maxChapters,
                start_url: startUrl || null,
            }),
        });
        if (!res.ok) {
            const err = await res.json();
            alert(err.detail || "Failed to add chapters");
            return;
        }
        document.getElementById("add-chapters-start-url").value = "";
        document.getElementById("add-chapters-count").value = "";
        addChaptersModal.classList.add("hidden");
        await loadNovelJobs(currentNovel.id);
        startJobPolling();
    } catch (e) {
        console.error("Failed to add chapters:", e);
        alert("Failed to add chapters");
    }
});

// ===================== Listened Chapters Tracking =====================

function loadListenedChapters() {
    try {
        return JSON.parse(localStorage.getItem("listened_chapters") || "{}");
    } catch {
        return {};
    }
}

function saveListenedChapters() {
    localStorage.setItem("listened_chapters", JSON.stringify(listenedChapters));
}

function markChapterListened(novelId, chapterNumber) {
    if (!listenedChapters[novelId]) listenedChapters[novelId] = [];
    if (!listenedChapters[novelId].includes(chapterNumber)) {
        listenedChapters[novelId].push(chapterNumber);
        saveListenedChapters();
    }
}

function isChapterListened(novelId, chapterNumber) {
    return listenedChapters[novelId]?.includes(chapterNumber) || false;
}

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

// ===================== Resume Listening =====================

const resumeBanner = document.getElementById("resume-banner");
const resumeDetail = document.getElementById("resume-detail");

document.getElementById("btn-dismiss-resume").addEventListener("click", () => {
    resumeBanner.classList.add("hidden");
});

document.getElementById("btn-resume-listening").addEventListener("click", async () => {
    // Use the same localStorage source as the banner for consistency
    const local = loadPlaybackPositionLocal();
    if (!local || !local.novel_id) return;

    const novel = novels.find((n) => String(n.id) === String(local.novel_id));
    if (!novel) return;

    resumeBanner.classList.add("hidden");
    await openNovel(novel);
    await resumePlaybackForNovel(novel.id);
});

async function resumePlaybackForNovel(novelId) {
    const pos = await loadPlaybackPosition(novelId);
    if (!pos || !pos.chapter_number) return;

    const chapter = chapters.find(
        (c) => c.chapter_number === pos.chapter_number && c.status === "audio_ready",
    );
    if (!chapter) return;

    currentChapter = chapter;
    const currentSpeed = pos.playback_speed || parseFloat(speedControl.value);
    audio.src = `${API_BASE}/novels/${currentNovel.id}/chapters/${chapter.chapter_number}/audio`;
    audio.load();
    audio.playbackRate = currentSpeed;
    speedControl.value = currentSpeed;
    speedDisplay.textContent = `${currentSpeed.toFixed(1)}x`;

    playerNovelTitle.textContent = currentNovel.title;
    playerChapterTitle.textContent = chapter.title_english
        ? chapter.title_english
        : chapter.title && chapter.title !== "Untitled"
            ? chapter.title
            : `Chapter ${chapter.chapter_number}`;
    playerBar.classList.remove("hidden");

    // Seek to saved position once metadata loads
    audio.addEventListener("loadedmetadata", function seekOnce() {
        audio.removeEventListener("loadedmetadata", seekOnce);
        if (pos.position_seconds && pos.position_seconds < audio.duration) {
            audio.currentTime = pos.position_seconds;
        }
    });

    updateMediaSession();
    renderChapterList();
    // Don't auto-play on resume — let the user press play
}

async function showResumeBanner() {
    // Check localStorage first for fastest display
    const local = loadPlaybackPositionLocal();
    if (local && local.novel_id && (local.position_seconds > 0 || local.chapter_number > 1)) {
        const novel = novels.find((n) => String(n.id) === String(local.novel_id));
        if (novel) {
            const chTitle = `Chapter ${local.chapter_number}`;
            const timeStr = local.position_seconds ? ` at ${formatTime(local.position_seconds)}` : "";
            resumeDetail.textContent = `${novel.title} — ${chTitle}${timeStr}`;
            resumeBanner.classList.remove("hidden");
            return;
        }
    }

    // Fall back to server API
    const lastNovelId = localStorage.getItem("last_novel_id");
    if (!lastNovelId) return;

    const novel = novels.find((n) => String(n.id) === String(lastNovelId));
    if (!novel) return;

    const pos = await loadPlaybackPosition(novel.id);
    if (!pos || (!pos.position_seconds && pos.chapter_number <= 1)) return;

    const chTitle = `Chapter ${pos.chapter_number}`;
    const timeStr = pos.position_seconds ? ` at ${formatTime(pos.position_seconds)}` : "";
    resumeDetail.textContent = `${novel.title} — ${chTitle}${timeStr}`;
    resumeBanner.classList.remove("hidden");
}

// ===================== Init =====================

async function init() {
    await loadNovels();
    connectWebSocket();
    await showResumeBanner();
}

init();
