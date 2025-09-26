from __future__ import annotations

from server.ingest import youtube


def test_normalize_youtube_url_variants() -> None:
    video_id = "dQw4w9WgXcQ"
    expected = f"https://www.youtube.com/watch?v={video_id}"

    assert youtube.normalize_youtube_url(f"https://youtu.be/{video_id}") == expected
    assert (
        youtube.normalize_youtube_url(
            f"https://www.youtube.com/watch?v={video_id}&feature=share"
        )
        == expected
    )
    assert (
        youtube.normalize_youtube_url(
            f"https://www.youtube.com/embed/{video_id}?si=z"
        )
        == expected
    )
    assert (
        youtube.normalize_youtube_url(
            f"https://www.youtube.com/shorts/{video_id}?feature=share"
        )
        == expected
    )
    assert youtube.normalize_youtube_url("https://example.com/video") is None


def test_extract_candidates_prefers_canonical_link() -> None:
    html = """
    <html>
      <head>
        <link rel="canonical" href="https://youtu.be/dQw4w9WgXcQ" />
        <meta property="og:video" content="https://www.youtube.com/watch?v=AAAAAAAAAAA" />
      </head>
      <body>
        <a href="https://www.youtube.com/watch?v=BBBBBBBBBBB">Video Link</a>
      </body>
    </html>
    """

    candidates = youtube._extract_candidates(html, "https://example.com/post")
    assert candidates[0] == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    assert "https://www.youtube.com/watch?v=AAAAAAAAAAA" in candidates


def test_extract_candidates_from_iframe_and_inline_text() -> None:
    html = """
    <html>
      <body>
        <iframe src="//www.youtube.com/embed/ZZZZZZZZZZZ"></iframe>
        <script>
          const url = "https://youtu.be/XXXXXXXXXXX";
        </script>
      </body>
    </html>
    """

    candidates = youtube._extract_candidates(html, "https://example.com/notes")
    assert "https://www.youtube.com/watch?v=ZZZZZZZZZZZ" in candidates
    assert "https://www.youtube.com/watch?v=XXXXXXXXXXX" in candidates
