const state = {
  claims: [],
  episodes: [],
  filteredClaims: [],
  pageSize: 6,
  currentPage: 1,
  query: "",
};

const searchForm = document.querySelector("#search-form");
const searchInput = document.querySelector("#search-query");
const filtersSection = document.querySelector("#filters");
const resultsSection = document.querySelector("#results");
const pagination = document.querySelector("#pagination");
const pagePrev = document.querySelector("#page-prev");
const pageNext = document.querySelector("#page-next");
const pageInfo = document.querySelector("#page-info");
const gradeSelect = document.querySelector("#filter-grade");
const domainWrapper = document.querySelector("#domain-filter");
const domainSelect = document.querySelector("#filter-domain");
const riskSelect = document.querySelector("#filter-risk");
const claimTemplate = document.querySelector("#claim-template");

function setResultsContent(node) {
  resultsSection.replaceChildren(node);
}

function createStatusMessage(message) {
  const p = document.createElement("p");
  p.className = "results__status";
  p.textContent = message;
  return p;
}

function normalize(value) {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function buildBadge(text, modifier) {
  if (!text) return null;
  const span = document.createElement("span");
  span.className = modifier ? `badge badge--${modifier}` : "badge";
  span.textContent = text;
  return span;
}

function renderEpisodes() {
  if (!state.episodes || state.episodes.length === 0) {
    return null;
  }
  const container = document.createElement("section");
  container.className = "results__episodes";
  const heading = document.createElement("h2");
  heading.textContent = `Episodes (${state.episodes.length})`;
  container.appendChild(heading);
  const list = document.createElement("ul");
  list.className = "results__episodes-list";
  for (const episode of state.episodes) {
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.href = `/episodes/${episode.id}`;
    link.textContent = episode.title;
    link.target = "_blank";
    link.rel = "noopener";
    item.appendChild(link);
    list.appendChild(item);
  }
  container.appendChild(list);
  return container;
}

function renderClaims() {
  const total = state.filteredClaims.length;
  if (total === 0) {
    pagination.hidden = true;
    pagination.setAttribute("aria-hidden", "true");
    setResultsContent(
      createStatusMessage(
        state.query
          ? `No claims found for "${state.query}". Try different terms or remove filters.`
          : "Start by searching for a topic above."
      )
    );
    return;
  }

  const startIndex = (state.currentPage - 1) * state.pageSize;
  const pageItems = state.filteredClaims.slice(startIndex, startIndex + state.pageSize);

  const fragment = document.createDocumentFragment();
  const episodesNode = renderEpisodes();
  if (episodesNode && state.currentPage === 1) {
    fragment.appendChild(episodesNode);
  }

  for (const claim of pageItems) {
    const node = claimTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector(".claim__title").textContent = claim.raw_text;
    const badges = node.querySelector(".claim__badges");

    if (claim.grade) {
      const gradeBadge = buildBadge(claim.grade, "grade");
      if (gradeBadge) badges.appendChild(gradeBadge);
    }

    if (claim.domain) {
      const domainBadge = buildBadge(claim.domain);
      if (domainBadge) badges.appendChild(domainBadge);
    }

    if (claim.risk_level) {
      const riskBadge = buildBadge(`${claim.risk_level} risk`, "risk");
      if (riskBadge) badges.appendChild(riskBadge);
    }

    node.querySelector(".claim__episode").textContent = claim.episode_title ?? "Episode";
    node.querySelector(".claim__topic").textContent = claim.topic ?? "—";
    const claimLink = node.querySelector(".claim__link");
    claimLink.href = `/claims/${claim.id}`;
    claimLink.textContent = "View claim details";

    fragment.appendChild(node);
  }

  setResultsContent(fragment);

  const pageCount = Math.ceil(total / state.pageSize);
  if (pageCount > 1) {
    pagination.hidden = false;
    pagination.removeAttribute("aria-hidden");
    pagePrev.disabled = state.currentPage === 1;
    pageNext.disabled = state.currentPage === pageCount;
    pageInfo.textContent = `Page ${state.currentPage} of ${pageCount}`;
  } else {
    pagination.hidden = true;
    pagination.setAttribute("aria-hidden", "true");
  }
}

function applyFilters() {
  const grade = normalize(gradeSelect.value);
  const domain = normalize(domainSelect.value);
  const risk = normalize(riskSelect.value);

  const filtered = state.claims.filter((claim) => {
    if (grade && normalize(claim.grade) !== grade) return false;
    if (domain && normalize(claim.domain) !== domain) return false;
    if (risk && normalize(claim.risk_level) !== risk) return false;
    return true;
  });

  state.filteredClaims = filtered;
  state.currentPage = 1;
  renderClaims();
}

function populateDomainOptions(claims) {
  const domains = Array.from(
    new Set(
      claims
        .map((item) => item.domain)
        .filter((value) => typeof value === "string" && value.trim().length > 0)
        .map((value) => value.trim())
    )
  ).sort((a, b) => a.localeCompare(b));

  domainSelect.replaceChildren();
  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = "All domains";
  domainSelect.appendChild(allOption);

  for (const domain of domains) {
    const option = document.createElement("option");
    option.value = domain;
    option.textContent = domain;
    domainSelect.appendChild(option);
  }

  if (domains.length > 0) {
    domainWrapper.hidden = false;
  } else {
    domainWrapper.hidden = true;
  }
}

async function runSearch(query) {
  state.query = query;
  state.currentPage = 1;
  filtersSection.hidden = true;
  pagination.hidden = true;
  pagination.setAttribute("aria-hidden", "true");
  setResultsContent(createStatusMessage("Searching…"));

  try {
    const response = await fetch(`/search?q=${encodeURIComponent(query)}`);
    if (!response.ok) {
      throw new Error(`Search request failed with status ${response.status}`);
    }
    const payload = await response.json();
    const claims = Array.isArray(payload.claims) ? payload.claims : [];
    const episodes = Array.isArray(payload.episodes) ? payload.episodes : [];

    state.claims = claims;
    state.episodes = episodes;
    populateDomainOptions(claims);

    if (claims.length > 0) {
      filtersSection.hidden = false;
    } else {
      filtersSection.hidden = true;
    }

    gradeSelect.value = "";
    domainSelect.value = "";
    riskSelect.value = "";

    state.filteredClaims = claims.slice();
    renderClaims();
  } catch (error) {
    console.error(error);
    setResultsContent(
      createStatusMessage("Something went wrong while searching. Please try again.")
    );
  }
}

searchForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const query = searchInput.value.trim();
  if (query.length < 2) {
    setResultsContent(createStatusMessage("Enter at least two characters to search."));
    return;
  }
  runSearch(query);
});

gradeSelect.addEventListener("change", () => applyFilters());
domainSelect.addEventListener("change", () => applyFilters());
riskSelect.addEventListener("change", () => applyFilters());

pagePrev.addEventListener("click", () => {
  if (state.currentPage > 1) {
    state.currentPage -= 1;
    renderClaims();
  }
});

pageNext.addEventListener("click", () => {
  const pageCount = Math.ceil(state.filteredClaims.length / state.pageSize);
  if (state.currentPage < pageCount) {
    state.currentPage += 1;
    renderClaims();
  }
});

// Restore query from hash fragment (?q=) if present
const params = new URLSearchParams(window.location.search);
const initialQuery = params.get("q");
if (initialQuery && initialQuery.trim().length >= 2) {
  searchInput.value = initialQuery;
  runSearch(initialQuery.trim());
}
