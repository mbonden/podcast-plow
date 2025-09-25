export const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "") ?? "http://localhost:8000";

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json"
    },
    ...init
  });

  if (!response.ok) {
    const errorText = await response.text().catch(() => "");
    throw new Error(errorText || response.statusText);
  }

  return (await response.json()) as T;
}

export interface EpisodeSummaryResponse {
  id: number;
  title: string;
  summary: {
    tl_dr: string | null;
    narrative: string | null;
  } | null;
  claims: ClaimSummary[];
}

export interface ClaimSummary {
  id: number;
  raw_text: string;
  normalized_text: string | null;
  topic: string | null;
  domain: string | null;
  risk_level: string | null;
  start_ms: number | null;
  end_ms: number | null;
  grade: ClaimGrade | null;
  grade_rationale: string | null;
}

export type ClaimGrade = "strong" | "moderate" | "weak" | "unsupported" | string | null;

export interface TopicClaimsResponse {
  topic: string;
  claims: TopicClaim[];
}

export interface TopicClaim {
  claim_id: number;
  episode_id: number;
  episode_title: string;
  raw_text: string;
  normalized_text: string | null;
  domain: string | null;
  risk_level: string | null;
  start_ms: number | null;
  end_ms: number | null;
  grade: ClaimGrade;
  grade_rationale: string | null;
}

export interface ClaimDetailResponse {
  claim_id: number;
  episode_title: string;
  topic: string | null;
  domain: string | null;
  risk_level: string | null;
  raw_text: string;
  normalized_text: string | null;
  grade: ClaimGrade;
  grade_rationale: string | null;
  rubric_version: string | null;
  graded_at: string | null;
  evidence: EvidenceItem[];
}

export interface EvidenceItem {
  id: number;
  title: string;
  year: number | null;
  type: string | null;
  journal: string | null;
  doi: string | null;
  pubmed_id: string | null;
  url: string | null;
  stance: string | null;
}

export interface SearchResponse {
  q: string;
  episodes: Array<{ id: number; title: string }>;
  claims: Array<{ id: number; raw_text: string; topic: string | null }>;
}

export function getEpisode(episodeId: string | number) {
  return fetchJson<EpisodeSummaryResponse>(`/episodes/${episodeId}`);
}

export function getTopicClaims(topic: string) {
  return fetchJson<TopicClaimsResponse>(`/topics/${encodeURIComponent(topic)}/claims`);
}

export function getClaim(claimId: string | number) {
  return fetchJson<ClaimDetailResponse>(`/claims/${claimId}`);
}

export function search(query: string) {
  const params = new URLSearchParams({ q: query });
  return fetchJson<SearchResponse>(`/search?${params.toString()}`);
}
