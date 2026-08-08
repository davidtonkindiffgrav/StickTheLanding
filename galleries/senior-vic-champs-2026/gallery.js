/* gallery.js — Senior Victorian Championship 2026 */

const BASE_URL    = 'https://stickthelanding.com.au/galleries/senior-vic-champs-2026';
const FULL_PATH   = 'photos/full/';
const THUMB_PATH  = 'photos/thumbs/';
const SLIDE_DELAY = 5500; // ms between slides

let photos = [];
let currentIndex = 0;
let slideIndex   = 0;
let slideTimer   = null;
let slidePaused  = false;
let activeSlot   = 'a';

// ── Element refs ──
const grid      = document.getElementById('gallery-grid');
const lightbox  = document.getElementById('lightbox');
const lbImg     = document.getElementById('lb-img');
const lbDl      = document.getElementById('lb-download');
const lbCopy    = document.getElementById('lb-copy');
const lbPrev    = document.getElementById('lb-prev');
const lbNext    = document.getElementById('lb-next');
const lbClose   = document.getElementById('lb-close');
const lbOverlay = document.getElementById('lightbox-overlay');
const toast     = document.getElementById('lb-toast');

const slideshow   = document.getElementById('slideshow');
const ssImgA      = document.getElementById('ss-img-a');
const ssImgB      = document.getElementById('ss-img-b');
const ssAudio     = document.getElementById('ss-audio');
const ssPlayPause = document.getElementById('ss-play-pause');
const ssClose     = document.getElementById('ss-close');
const ssCounter   = document.getElementById('ss-counter');
const btnPlay     = document.getElementById('btn-play');

// ── Bootstrap ──
fetch('photos.json')
  .then(r => { if (!r.ok) throw new Error('not found'); return r.json(); })
  .then(data => {
    photos = data;
    buildGrid();
    checkDeepLink();
  })
  .catch(() => {
    grid.innerHTML = '<p class="gallery-loading">No photos loaded yet. Add images to <code>photos/full/</code> and run <code>generate_manifest.py</code>.</p>';
  });

// ── Grid ──
function buildGrid() {
  grid.innerHTML = '';
  photos.forEach((filename, i) => {
    const item = document.createElement('div');
    item.className = 'gallery-item';
    item.tabIndex = 0;
    item.setAttribute('role', 'button');
    item.setAttribute('aria-label', `Open photo ${i + 1}`);

    const img = document.createElement('img');
    img.src     = THUMB_PATH + filename;
    img.alt     = `Photo ${i + 1} of ${photos.length}`;
    img.loading = 'lazy';
    img.decoding = 'async';

    item.appendChild(img);
    item.addEventListener('click', () => openLightbox(i));
    item.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') openLightbox(i); });
    grid.appendChild(item);
  });
}

// ── Lightbox ──
function openLightbox(index) {
  currentIndex = index;
  updateLightboxContent();
  lightbox.classList.add('active');
  lightbox.setAttribute('aria-hidden', 'false');
  history.replaceState(null, '', `#photo-${index + 1}`);
  document.body.style.overflow = 'hidden';
  lbClose.focus();
}

function closeLightbox() {
  lightbox.classList.remove('active');
  lightbox.setAttribute('aria-hidden', 'true');
  history.replaceState(null, '', window.location.pathname + window.location.search);
  document.body.style.overflow = '';
  lbImg.src = '';
}

function updateLightboxContent() {
  const filename = photos[currentIndex];
  lbImg.src = FULL_PATH + filename;
  lbImg.alt = `Photo ${currentIndex + 1} of ${photos.length}`;
  lbDl.setAttribute('data-filename', filename);
  lbPrev.disabled = currentIndex === 0;
  lbNext.disabled = currentIndex === photos.length - 1;
}

lbClose.addEventListener('click', closeLightbox);
lbOverlay.addEventListener('click', closeLightbox);

lbPrev.addEventListener('click', () => {
  if (currentIndex > 0) { currentIndex--; updateLightboxContent(); }
});
lbNext.addEventListener('click', () => {
  if (currentIndex < photos.length - 1) { currentIndex++; updateLightboxContent(); }
});

// Download — fetch+blob ensures the save dialog appears (not a browser tab open)
lbDl.addEventListener('click', e => {
  e.preventDefault();
  const filename = photos[currentIndex];
  fetch(FULL_PATH + filename)
    .then(r => r.blob())
    .then(blob => {
      const url = URL.createObjectURL(blob);
      const a   = Object.assign(document.createElement('a'), { href: url, download: filename });
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 15000);
    })
    .catch(() => showToast('Download failed. Try right-clicking the image.'));
});

// Copy direct image link
lbCopy.addEventListener('click', () => {
  const filename = photos[currentIndex];
  const url      = `${BASE_URL}/photos/full/${filename}`;
  if (navigator.clipboard) {
    navigator.clipboard.writeText(url)
      .then(() => showToast('Link copied!'))
      .catch(() => fallbackCopy(url));
  } else {
    fallbackCopy(url);
  }
});

function fallbackCopy(text) {
  const ta = Object.assign(document.createElement('textarea'), {
    value: text, style: 'position:fixed;opacity:0'
  });
  document.body.appendChild(ta);
  ta.select();
  try { document.execCommand('copy'); showToast('Link copied!'); }
  catch { prompt('Copy this link:', text); }
  document.body.removeChild(ta);
}

function showToast(msg) {
  toast.textContent = msg;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 2400);
}

// ── Slideshow ──
btnPlay.addEventListener('click', openSlideshow);
ssClose.addEventListener('click', closeSlideshow);

function openSlideshow() {
  if (!photos.length) return;
  slideIndex  = 0;
  slidePaused = false;
  activeSlot  = 'a';

  ssImgA.className = 'ss-img active';
  ssImgA.src       = FULL_PATH + photos[0];
  ssImgB.className = 'ss-img';
  ssImgB.src       = '';
  updateSsCounter();

  slideshow.classList.add('active');
  slideshow.setAttribute('aria-hidden', 'false');
  document.body.style.overflow = 'hidden';

  ssAudio.load();
  ssAudio.play().catch(() => {});
  ssPlayPause.innerHTML = '<i class="fas fa-pause"></i>';
  ssPlayPause.title = 'Pause';

  scheduleNextSlide();
}

function closeSlideshow() {
  clearTimeout(slideTimer);
  ssAudio.pause();
  slideshow.classList.remove('active');
  slideshow.setAttribute('aria-hidden', 'true');
  document.body.style.overflow = '';
  // Clear images after transition
  setTimeout(() => { ssImgA.src = ''; ssImgB.src = ''; }, 400);
}

function scheduleNextSlide() {
  clearTimeout(slideTimer);
  if (!slidePaused) slideTimer = setTimeout(advanceSlide, SLIDE_DELAY);
}

function advanceSlide() {
  slideIndex = (slideIndex + 1) % photos.length;
  const nextSrc = FULL_PATH + photos[slideIndex];

  const incoming = activeSlot === 'a' ? ssImgB : ssImgA;
  const outgoing = activeSlot === 'a' ? ssImgA : ssImgB;

  incoming.src = nextSrc;
  incoming.onload = () => {
    incoming.className = 'ss-img active';
    outgoing.className = 'ss-img';
    activeSlot = activeSlot === 'a' ? 'b' : 'a';
    updateSsCounter();
    scheduleNextSlide();
  };
  incoming.onerror = scheduleNextSlide;
}

function updateSsCounter() {
  ssCounter.textContent = `${slideIndex + 1} / ${photos.length}`;
}

ssPlayPause.addEventListener('click', () => {
  slidePaused = !slidePaused;
  if (slidePaused) {
    clearTimeout(slideTimer);
    ssAudio.pause();
    ssPlayPause.innerHTML = '<i class="fas fa-play"></i>';
    ssPlayPause.title = 'Play';
  } else {
    ssAudio.play().catch(() => {});
    ssPlayPause.innerHTML = '<i class="fas fa-pause"></i>';
    ssPlayPause.title = 'Pause';
    scheduleNextSlide();
  }
});

// ── Deep link (#photo-N) ──
function checkDeepLink() {
  const m = window.location.hash.match(/^#photo-(\d+)$/);
  if (m) {
    const idx = parseInt(m[1], 10) - 1;
    if (idx >= 0 && idx < photos.length) openLightbox(idx);
  }
}

// ── Keyboard ──
document.addEventListener('keydown', e => {
  if (lightbox.classList.contains('active')) {
    if (e.key === 'Escape')      { closeLightbox(); return; }
    if (e.key === 'ArrowLeft')   { lbPrev.click(); return; }
    if (e.key === 'ArrowRight')  { lbNext.click(); return; }
  }
  if (slideshow.classList.contains('active') && e.key === 'Escape') closeSlideshow();
});
