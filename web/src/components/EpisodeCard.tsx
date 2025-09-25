import { Link } from "react-router-dom";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "./ui/card";
import { Button } from "./ui/button";
import type { EpisodeSummaryResponse } from "../lib/api";
import ClaimCard from "./ClaimCard";

interface EpisodeCardProps {
  episode: EpisodeSummaryResponse;
  showFooter?: boolean;
}

export default function EpisodeCard({ episode, showFooter = true }: EpisodeCardProps) {
  return (
    <Card className="overflow-hidden">
      <CardHeader>
        <CardTitle className="text-2xl font-semibold leading-tight">{episode.title}</CardTitle>
        {episode.summary?.tl_dr && (
          <CardDescription className="mt-2 text-base">{episode.summary.tl_dr}</CardDescription>
        )}
      </CardHeader>
      <CardContent className="space-y-6">
        {episode.summary?.narrative && (
          <section>
            <h4 className="mb-2 text-sm font-semibold uppercase text-muted-foreground">Narrative</h4>
            <p className="leading-relaxed text-muted-foreground">{episode.summary.narrative}</p>
          </section>
        )}
        <section>
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-semibold uppercase text-muted-foreground">Claims</h4>
            <span className="text-xs text-muted-foreground">{episode.claims.length} total</span>
          </div>
          <div className="mt-4 grid gap-4">
            {episode.claims.map((claim) => (
              <ClaimCard key={claim.id} claim={claim} showEpisodeLink={false} />
            ))}
          </div>
        </section>
      </CardContent>
      {showFooter && (
        <CardFooter className="flex justify-end">
          <Button asChild variant="secondary">
            <Link to={`/episode/${episode.id}`}>View episode</Link>
          </Button>
        </CardFooter>
      )}
    </Card>
  );
}
