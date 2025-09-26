import { Link } from "react-router-dom";
import type { ClaimDetailResponse, ClaimSummary, TopicClaim } from "../lib/api";
import { Badge } from "./ui/badge";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "./ui/card";
import { Button } from "./ui/button";

export type ClaimLike = ClaimSummary | TopicClaim | ClaimDetailResponse;

interface ClaimCardProps {
  claim: ClaimLike;
  showEpisodeLink?: boolean;
  episodeId?: number;
  episodeTitle?: string;
}

function gradeVariant(grade: string | null | undefined) {
  const normalized = grade?.toLowerCase();
  switch (normalized) {
    case "strong":
      return { variant: "success", label: "Strong" } as const;
    case "moderate":
      return { variant: "secondary", label: "Moderate" } as const;
    case "weak":
      return { variant: "warning", label: "Weak" } as const;
    case "unsupported":
      return { variant: "danger", label: "Unsupported" } as const;
    default:
      return { variant: "outline", label: grade ? grade : "Unreviewed" } as const;
  }
}

function isTopicClaim(value: ClaimLike): value is TopicClaim {
  return "claim_id" in value && "episode_id" in value;
}

function isClaimDetail(value: ClaimLike): value is ClaimDetailResponse {
  return "claim_id" in value && "episode_title" in value && !("episode_id" in value);
}

export default function ClaimCard({ claim, showEpisodeLink = true, episodeId, episodeTitle }: ClaimCardProps) {
  const grade = gradeVariant(claim.grade ?? null);
  const claimId = "claim_id" in claim ? claim.claim_id : claim.id;
  const title = "raw_text" in claim ? claim.raw_text : "";
  const normalized = "normalized_text" in claim ? claim.normalized_text : null;
  const risk = "risk_level" in claim ? claim.risk_level : null;
  const domain = "domain" in claim ? claim.domain : null;
  const topic = "topic" in claim ? claim.topic : null;
  const resolvedEpisodeId = episodeId ?? (isTopicClaim(claim) ? claim.episode_id : undefined);
  const resolvedEpisodeTitle =
    episodeTitle ?? (isTopicClaim(claim) ? claim.episode_title : isClaimDetail(claim) ? claim.episode_title : undefined);
  const citations = isClaimDetail(claim) ? claim.evidence : [];

  return (
    <Card>
      <CardHeader className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <CardTitle className="text-base font-semibold leading-snug">{title}</CardTitle>
        <Badge variant={grade.variant}>{grade.label}</Badge>
      </CardHeader>
      <CardContent className="space-y-3 text-sm text-muted-foreground">
        {normalized && (
          <p>
            <span className="font-medium text-foreground">Normalized:</span> {normalized}
          </p>
        )}
        <div className="flex flex-wrap gap-4 text-xs uppercase tracking-wide text-muted-foreground">
          {topic && <span>Topic: {topic}</span>}
          {domain && <span>Domain: {domain}</span>}
          {risk && <span>Risk: {risk}</span>}
        </div>
        {"grade_rationale" in claim && claim.grade_rationale && (
          <p>
            <span className="font-medium text-foreground">Rationale:</span> {claim.grade_rationale}
          </p>
        )}
        {citations.length > 0 && (
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Citations</p>
            <ul className="mt-1 space-y-1 text-sm text-muted-foreground">
              {citations.slice(0, 3).map((item) => (
                <li key={item.id} className="list-inside list-disc">
                  <span className="text-foreground">{item.title}</span>
                  {item.year ? ` (${item.year})` : ""}
                </li>
              ))}
              {citations.length > 3 && (
                <li className="text-xs text-muted-foreground">+ {citations.length - 3} more source(s)</li>
              )}
            </ul>
          </div>
        )}
      </CardContent>
      <CardFooter className="flex flex-wrap items-center gap-2">
        <Button asChild size="sm">
          <Link to={`/claim/${claimId}`}>View claim</Link>
        </Button>
        {showEpisodeLink && resolvedEpisodeId && (
          <Button asChild variant="ghost" size="sm" className="text-xs">
            <Link to={`/episode/${resolvedEpisodeId}`}>{resolvedEpisodeTitle ?? "Episode"}</Link>
          </Button>
        )}
      </CardFooter>
    </Card>
  );
}
