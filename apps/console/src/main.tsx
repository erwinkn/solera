import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MutationCache, QueryCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { ApiError } from "@/api/client";
import { makeRouter } from "@/router";
import "@/styles/app.css";

const unauthorized = (error: unknown) => error instanceof ApiError && error.status === 401;

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1_000,
      refetchIntervalInBackground: false,
      // A 404 or a refused token won't fix itself by asking again.
      retry: (count, error) => !(error instanceof ApiError && error.status < 500) && count < 2,
    },
  },
  queryCache: new QueryCache(),
  mutationCache: new MutationCache({
    onError: (error) => {
      if (unauthorized(error)) queryClient.cancelQueries();
    },
  }),
});

const router = makeRouter(queryClient);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);
