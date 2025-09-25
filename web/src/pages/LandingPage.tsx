import { FormEvent, useState } from "react";
import { Link } from "react-router-dom";
import { search, type SearchResponse } from "../lib/api";
import { Input } from "../components/ui/input";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { Skeleton } from "../components/ui/skeleton";

export default function LandingPage() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (query.trim().length < 2) {
      setError("Enter at least two characters to search.");
      setResults(null);
      return;
    }
    try {
      setIsLoading(true);
      setError(null);
      const data = await search(query.trim());
      setResults(data);
    } catch (err) {
      console.error(err);
      setError(err instanceof Error ? err.message : "Unable to search right now.");
      setResults(null);
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="space-y-10">
      <section className="space-y-4 text-center">
        <h1 className="text-4xl font-bold tracking-tight">Podcast fact checking at a glance</h1>
        <p className="mx-auto max-w-2xl text-lg text-muted-foreground">
          Search recent episodes and dive into claims, automated grades, and supporting evidence from the Podcast Plow backend.
        </p>
      </section>

      <form onSubmit={handleSearch} className="mx-auto flex max-w-xl items-center gap-2">
        <Input
          placeholder="Search for episodes or claims..."
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <Button type="submit" disabled={isLoading}>
          {isLoading ? "Searching..." : "Search"}
        </Button>
      </form>

      {error && <p className="text-center text-sm text-destructive">{error}</p>}

      <section className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Episodes</CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <Skeleton className="h-24 w-full" />
            ) : results?.episodes.length ? (
              <ul className="space-y-3 text-sm">
                {results.episodes.map((episode) => (
                  <li key={episode.id}>
                    <Link className="text-primary hover:underline" to={`/episode/${episode.id}`}>
                      {episode.title}
                    </Link>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-sm text-muted-foreground">Search to see matching episodes.</p>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Claims</CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <Skeleton className="h-24 w-full" />
            ) : results?.claims.length ? (
              <ul className="space-y-3 text-sm">
                {results.claims.map((claim) => (
                  <li key={claim.id}>
                    <Link className="text-primary hover:underline" to={`/claim/${claim.id}`}>
                      {claim.raw_text}
                    </Link>
                    {claim.topic && <span className="ml-2 text-xs uppercase text-muted-foreground">{claim.topic}</span>}
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-sm text-muted-foreground">Search to see matching claims.</p>
            )}
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
