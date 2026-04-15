import { DependencyList, useEffect, useState } from "react";

type AsyncState<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

const EMPTY_DEPS: DependencyList = [];

export function useAsyncData<T>(loader: () => Promise<T>, deps: DependencyList = EMPTY_DEPS) {
  const [state, setState] = useState<AsyncState<T>>({
    data: null,
    loading: true,
    error: null,
  });

  useEffect(() => {
    let active = true;

    setState({ data: null, loading: true, error: null });

    Promise.resolve()
      .then(loader)
      .then((data) => {
        if (active) {
          setState({ data, loading: false, error: null });
        }
      })
      .catch((error: unknown) => {
        if (active) {
          setState({
            data: null,
            loading: false,
            error: getErrorMessage(error),
          });
        }
      });

    return () => {
      active = false;
    };
  }, deps);

  return state;
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  try {
    return JSON.stringify(error) ?? "Unknown error";
  } catch {
    return "Unknown error";
  }
}
