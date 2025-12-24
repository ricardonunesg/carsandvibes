import { component$, useContext, useStore } from '@qwik.dev/core';
import { _ } from 'compiled-i18n';
import { APP_STATE } from '~/constants';
import CloseIcon from '../icons/CloseIcon';

export default component$(() => {
  const appState = useContext(APP_STATE);

  const ui = useStore<{ openParentId: string | null }>({
    openParentId: null,
  });

  const collections = appState.collections;

  const ROOT_ID = '1';
  const parents = collections.filter(
    (c) => c.parent?.id === ROOT_ID || c.parent?.name === '__root_collection__'
  );

  return (
    <>
      {appState.showCategoriesMenu && (
        <div class="fixed inset-0 overflow-hidden z-40">
          <div
            class="absolute inset-0 bg-gray-500 bg-opacity-75"
            onClick$={() => (appState.showCategoriesMenu = false)}
          />

          <div class="fixed inset-y-0 left-0 max-w-full flex">
            <div class="w-screen max-w-md">
              <div class="h-full flex flex-col bg-white shadow-xl overflow-y-auto">
                <div class="px-4 py-4 flex items-center justify-between border-b">
                  <h2 class="text-lg font-medium text-gray-900">{_`Categorias`}</h2>
                  <button
                    class="text-gray-400 hover:text-gray-600"
                    onClick$={() => (appState.showCategoriesMenu = false)}
                  >
                    <CloseIcon />
                  </button>
                </div>

                <div class="px-4 py-6 space-y-4">
                  {parents.map((parent) => {
                    const children = collections.filter((c) =>
                      c.parent?.id
                        ? c.parent.id === parent.id
                        : c.parent?.name === parent.name
                    );

                    const isOpen = ui.openParentId === parent.id;

                    return (
                      <div key={parent.id} class="border-b pb-2">
                        <button
                          class="w-full flex justify-between items-center text-left font-medium text-gray-900"
                          onClick$={() => (ui.openParentId = isOpen ? null : parent.id)}
                        >
                          <span>{parent.name}</span>
                          {children.length > 0 && (
                            <span class="text-gray-400">{isOpen ? '−' : '+'}</span>
                          )}
                        </button>

                        {isOpen && children.length > 0 && (
                          <div class="mt-3 ml-3 space-y-2">
                            {children.map((child) => (
                              <a
                                key={child.id}
                                href={`/collections/${child.slug}`}
                                class="block text-sm text-gray-600 hover:text-gray-900"
                                onClick$={() => (appState.showCategoriesMenu = false)}
                              >
                                {child.name}
                              </a>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>

              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
});
