import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import EpisodeCard from "../components/EpisodeCard";
import ClaimCard from "../components/ClaimCard";
import { Skeleton } from "../components/ui/skeleton";
import { getEpisode, type EpisodeSummaryResponse } from "../lib/api";

export default function EpisodePage() {
  const { episodeId } = useParams();
  const [data, setData] = useState<EpisodeSummaryResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    const id = episodeId;
    if (!id) {
      setError("Missing episode id.");
      return;
    }
    async function loadEpisode(currentId: string) {
      try {
        setIsLoading(true);
        setError(null);
        const response = await getEpisode(currentId);
        if (isMounted) {
          setData(response);
        }
      } catch (err) {
        console.error(err);
        if (isMounted) {
          setError(err instanceof Error ? err.message : "Unable to load episode.");
          setData(null);
        }
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    }
    loadEpisode(id);
    return () => {
      isMounted = false;
    };
  }, [episodeId]);

  if (isLoading) {
    return <Skeleton className="h-64 w-full" />;
  }

  if (error) {
    return <p className="text-sm text-destructive">{error}</p>;
  }

  if (!data) {
    return <p className="text-sm text-muted-foreground">Episode not found.</p>;
  }

  return (
    <div className="space-y-8">
      <EpisodeCard episode={data} showFooter={false} />
      <section className="space-y-4">
        <h2 className="text-2xl font-semibold tracking-tight">Claims in this episode</h2>
        {data.claims.length ? (
          <div className="grid gap-4">
            {data.claims.map((claim) => (
              <ClaimCard key={claim.id} claim={claim} showEpisodeLink={false} episodeId={data.id} episodeTitle={data.title} />
            ))}
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">No claims have been extracted for this episode yet.</p>
        )}
      </section>
    </div>
  );
}
