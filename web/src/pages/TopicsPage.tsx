import { FormEvent, useMemo, useState } from "react";
import ClaimCard from "../components/ClaimCard";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Skeleton } from "../components/ui/skeleton";
import { getTopicClaims, type TopicClaimsResponse } from "../lib/api";

const suggestedTopics = ["health", "technology", "politics", "economics", "science"];

export default function TopicsPage() {
  const [query, setQuery] = useState("");
  const [selectedTopic, setSelectedTopic] = useState<string>("");
  const [data, setData] = useState<TopicClaimsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadTopic(topic: string) {
    if (!topic) {
      return;
    }
    try {
      setIsLoading(true);
      setError(null);
      const response = await getTopicClaims(topic);
      setData(response);
      setSelectedTopic(topic);
    } catch (err) {
      console.error(err);
      setError(err instanceof Error ? err.message : "Unable to load topic claims.");
      setData(null);
    } finally {
      setIsLoading(false);
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    loadTopic(query.trim());
  }

  const emptyStateMessage = useMemo(() => {
    if (!selectedTopic) {
      return "Pick a topic below to see related claims.";
    }
    if (data && data.claims.length === 0) {
      return `No claims found for “${selectedTopic}” yet.`;
    }
    return null;
  }, [data, selectedTopic]);

  return (
    <div className="space-y-8">
      <section className="space-y-3">
        <h1 className="text-3xl font-semibold tracking-tight">Browse by topic</h1>
        <p className="text-muted-foreground">
          Explore claims that mention a specific topic. Suggestions below match the API response from
          <code className="mx-1 rounded bg-muted px-1.5 py-0.5 text-xs">/topics/&lt;topic&gt;/claims</code>.
        </p>
      </section>

      <form onSubmit={handleSubmit} className="flex flex-wrap items-center gap-3">
        <Input
          className="max-w-xs"
          placeholder="Enter a topic keyword"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <Button type="submit" disabled={isLoading || !query.trim()}>
          {isLoading ? "Loading..." : "Load claims"}
        </Button>
      </form>

      <div className="flex flex-wrap gap-2">
        {suggestedTopics.map((topic) => (
          <Button
            key={topic}
            variant={selectedTopic === topic ? "default" : "secondary"}
            size="sm"
            type="button"
            onClick={() => {
              setQuery(topic);
              loadTopic(topic);
            }}
          >
            {topic}
          </Button>
        ))}
      </div>

      {error && <p className="text-sm text-destructive">{error}</p>}

      {isLoading ? (
        <Skeleton className="h-32 w-full" />
      ) : data && data.claims.length > 0 ? (
        <div className="grid gap-4">
          {data.claims.map((claim) => (
            <ClaimCard key={claim.claim_id} claim={claim} />
          ))}
        </div>
      ) : (
        <p className="text-sm text-muted-foreground">
          {emptyStateMessage ?? "Pick a topic below to get started."}
        </p>
      )}
    </div>
  );
}
