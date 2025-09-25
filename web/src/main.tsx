import React from "react";
import ReactDOM from "react-dom/client";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import App from "./App";
import LandingPage from "./pages/LandingPage";
import TopicsPage from "./pages/TopicsPage";
import EpisodePage from "./pages/EpisodePage";
import ClaimPage from "./pages/ClaimPage";
import NotFoundPage from "./pages/NotFoundPage";
import "./index.css";

const router = createBrowserRouter([
  {
    path: "/",
    element: <App />,
    children: [
      {
        index: true,
        element: <LandingPage />
      },
      {
        path: "topics",
        element: <TopicsPage />
      },
      {
        path: "episode/:episodeId",
        element: <EpisodePage />
      },
      {
        path: "claim/:claimId",
        element: <ClaimPage />
      },
      {
        path: "*",
        element: <NotFoundPage />
      }
    ]
  }
]);

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
);
