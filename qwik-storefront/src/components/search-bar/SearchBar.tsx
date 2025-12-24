import { $, component$, useStore } from '@qwik.dev/core';
import { _ } from 'compiled-i18n';

type SearchItem = {
  productName?: string;
  slug?: string;
  productAsset?: { preview?: string | null } | null;
};

export default component$(() => {
  const state = useStore({
    q: '',
    loading: false,
    open: false,
    items: [] as SearchItem[],
    err: '',
    t: 0 as any,
  });

  const runSearch = $(async (term: string) => {
    state.err = '';
    const q = term.trim();

    if (q.length < 2) {
      state.items = [];
      state.open = false;
      return;
    }

    state.loading = true;

    try {
      // ⚠️ Query direta ao /shop-api (NGINX já faz proxy para Vendure)
      const query = `
        query Search($input: SearchInput!) {
          search(input: $input) {
            items {
              productName
              slug
              productAsset { preview }
            }
          }
        }
      `;

      const res = await fetch('/shop-api', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            input: {
              term: q,
              take: 8,
              groupByProduct: true
            },
          },
        }),
      });

      const json = await res.json();

      if (json?.errors?.length) {
        throw new Error(json.errors[0]?.message || 'Erro no search');
      }

      state.items = (json?.data?.search?.items || []) as SearchItem[];
      state.open = true;
    } catch (e: any) {
      state.err = e?.message ?? 'Erro no autocomplete';
      state.items = [];
      state.open = false;
    } finally {
      state.loading = false;
    }
  });

  const onInput = $((ev: Event) => {
    const value = (ev.target as HTMLInputElement).value;
    state.q = value;

    // debounce ~250ms
    clearTimeout(state.t);
    state.t = setTimeout(() => runSearch(value), 250);
  });

  return (
    <div class="relative">
      <form action="/search">
        <input
          type="search"
          name="q"
          value={state.q}
          placeholder={_`Search`}
          autoComplete="off"
          class="shadow-sm focus:ring-primary-500 focus:border-primary-500 block w-full sm:text-sm border-gray-300 rounded-md"
          onInput$={onInput}
          onFocus$={$(() => {
            if (state.items.length) state.open = true;
          })}
          onBlur$={$(() => {
            // delay para permitir click
            setTimeout(() => (state.open = false), 150);
          })}
        />
      </form>

      {state.open && (
        <div class="absolute z-50 mt-2 w-full rounded-md border border-gray-200 bg-white shadow-lg">
          {state.loading ? (
            <div class="p-3 text-sm text-gray-500">{_`Searching...`}</div>
          ) : state.err ? (
            <div class="p-3 text-sm text-red-600">{state.err}</div>
          ) : state.items.length === 0 ? (
            <div class="p-3 text-sm text-gray-500">{_`No results`}</div>
          ) : (
            <ul class="max-h-96 overflow-auto">
              {state.items.map((it, idx) => (
                <li key={idx}>
                  <a
                    href={it.slug ? `/products/${it.slug}/` : `/search?q=${encodeURIComponent(state.q)}`}
                    class="flex items-center gap-3 px-3 py-2 hover:bg-gray-50"
                  >
                    <div class="h-10 w-10 flex-shrink-0 overflow-hidden rounded bg-gray-100">
                      {it.productAsset?.preview ? (
                        <img
                          src={it.productAsset.preview}
                          alt={it.productName ?? 'Product'}
                          class="h-10 w-10 object-cover"
                        />
                      ) : null}
                    </div>

                    <div class="min-w-0">
                      <div class="truncate text-sm font-medium text-gray-900">
                        {it.productName}
                      </div>
                    </div>
                  </a>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
});
