import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import ClaimCard from "../components/ClaimCard";
import EvidenceList from "../components/EvidenceList";
import { Skeleton } from "../components/ui/skeleton";
import { getClaim, type ClaimDetailResponse } from "../lib/api";

export default function ClaimPage() {
  const { claimId } = useParams();
  const [data, setData] = useState<ClaimDetailResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    const id = claimId;
    if (!id) {
      setError("Missing claim id.");
      return;
    }
    async function loadClaim(currentId: string) {
      try {
        setIsLoading(true);
        setError(null);
        const response = await getClaim(currentId);
        if (isMounted) {
          setData(response);
        }
      } catch (err) {
        console.error(err);
        if (isMounted) {
          setError(err instanceof Error ? err.message : "Unable to load claim.");
          setData(null);
        }
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    }
    loadClaim(id);
    return () => {
      isMounted = false;
    };
  }, [claimId]);

  if (isLoading) {
    return <Skeleton className="h-64 w-full" />;
  }

  if (error) {
    return <p className="text-sm text-destructive">{error}</p>;
  }

  if (!data) {
    return <p className="text-sm text-muted-foreground">Claim not found.</p>;
  }

  const gradedAt = data.graded_at ? new Date(data.graded_at).toLocaleString() : "—";

  return (
    <div className="space-y-8">
      <div className="space-y-1">
        <h1 className="text-3xl font-semibold tracking-tight">Claim overview</h1>
        <p className="text-sm text-muted-foreground">
          Episode: {data.episode_title}
        </p>
      </div>

      <ClaimCard claim={data} showEpisodeLink={false} episodeTitle={data.episode_title} />

      <section className="grid gap-2 rounded-lg border bg-card p-6 text-sm text-muted-foreground">
        <p>
          <span className="font-medium text-foreground">Topic:</span> {data.topic ?? "—"}
        </p>
        <p>
          <span className="font-medium text-foreground">Domain:</span> {data.domain ?? "—"}
        </p>
        <p>
          <span className="font-medium text-foreground">Risk level:</span> {data.risk_level ?? "—"}
        </p>
        <p>
          <span className="font-medium text-foreground">Rubric version:</span> {data.rubric_version ?? "—"}
        </p>
        <p>
          <span className="font-medium text-foreground">Graded at:</span> {gradedAt}
        </p>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="text-2xl font-semibold tracking-tight">Evidence</h2>
          <p className="text-sm text-muted-foreground">
            These sources were linked to the claim by the grading pipeline.
          </p>
        </div>
        <EvidenceList items={data.evidence} />
      </section>
    </div>
  );
}
