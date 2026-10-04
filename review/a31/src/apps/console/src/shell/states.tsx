import { useQueryErrorResetBoundary } from "@tanstack/react-query";
import { Link, useRouter, type ErrorComponentProps } from "@tanstack/react-router";
import { ApiError } from "@/api/client";
import { Button, buttonClass } from "@/ui/button";
import { Empty, Skeleton } from "@/ui/data";
import { Page } from "@/ui/layout";

export function PageLoading() {
  return (
    <Page aria-busy="true" aria-label="Loading">
      <div className="flex flex-col gap-3">
        <Skeleton className="h-3 w-24" />
        <Skeleton className="h-7 w-64" />
      </div>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {[0, 1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-24" />
        ))}
      </div>
      <Skeleton className="h-72" />
    </Page>
  );
}

export function PageError({ error, reset }: ErrorComponentProps) {
  const router = useRouter();
  const queries = useQueryErrorResetBoundary();
  const missing = error instanceof ApiError && error.status === 404;
  return (
    <Page>
      <Empty
        title={missing ? "Not found" : "Something went wrong"}
        action={
          <div className="flex gap-2">
            <Button
              variant="primary"
              onClick={() => {
                queries.reset();
                reset();
                void router.invalidate();
              }}
            >
              Try again
            </Button>
            <Link to="/" className={buttonClass("ghost")}>
              Overview
            </Link>
          </div>
        }
      >
        {missing
          ? "The server doesn't know this resource. It may have been deleted or renamed."
          : error instanceof Error
            ? error.message
            : String(error)}
      </Empty>
    </Page>
  );
}

export function NotFound() {
  return (
    <Page>
      <Empty
        title="Nothing at this address"
        action={
          <Link to="/" className={buttonClass("primary")}>
            Back to the overview
          </Link>
        }
      >
        The console has no page here.
      </Empty>
    </Page>
  );
}
