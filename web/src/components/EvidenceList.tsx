import type { EvidenceItem } from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Badge } from "./ui/badge";

interface EvidenceListProps {
  items: EvidenceItem[];
}

export default function EvidenceList({ items }: EvidenceListProps) {
  if (!items.length) {
    return <p className="text-sm text-muted-foreground">No evidence has been linked to this claim yet.</p>;
  }

  return (
    <div className="grid gap-4">
      {items.map((item) => (
        <Card key={item.id}>
          <CardHeader className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <CardTitle className="text-base font-semibold leading-tight">
                {item.title}
              </CardTitle>
              <p className="text-sm text-muted-foreground">
                {[item.journal, item.year].filter(Boolean).join(" • ") || "Unpublished"}
              </p>
            </div>
            {item.stance && (
              <Badge variant={item.stance.toLowerCase() === "supports" ? "success" : "outline"}>
                {item.stance}
              </Badge>
            )}
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            <div className="flex flex-wrap gap-3">
              {item.type && <Badge variant="secondary">{item.type}</Badge>}
              {item.doi && (
                <a href={`https://doi.org/${item.doi}`} className="text-primary hover:underline" target="_blank" rel="noreferrer">
                  DOI: {item.doi}
                </a>
              )}
              {item.pubmed_id && (
                <a
                  href={`https://pubmed.ncbi.nlm.nih.gov/${item.pubmed_id}/`}
                  className="text-primary hover:underline"
                  target="_blank"
                  rel="noreferrer"
                >
                  PubMed: {item.pubmed_id}
                </a>
              )}
              {item.url && (
                <a href={item.url} className="text-primary hover:underline" target="_blank" rel="noreferrer">
                  Source link
                </a>
              )}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
