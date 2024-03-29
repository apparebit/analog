import os.path
# We use a different regular expression implementation because matomo includes
# negative lookbehind with differing lengths, which isn't supported by the
# standard library package.
import regex
from ruyaml import YAML

import analog


class BotDetector:
    def __init__(self) -> None:
        # Instead of deprecated pkgutil package, use loader's deprecated get_data()
        if (spec := analog.__spec__) is None:
            raise ValueError('module "analog" has no spec')
        if (loader := spec.loader) is None:
            raise ValueError('module spec for "analog" has no loader')
        if not hasattr(loader, 'get_data'):
            raise ValueError('loader for "analog" does not implement get_data()')

        if (paths := analog.__path__) is None:
            raise ValueError('module "analog" does not represent a package')
        if len(paths) != 1:
            raise ValueError('package "analog" has more than one module search path')

        pkgpath = analog.__path__[0]
        bots_yml = os.path.join(pkgpath, 'bots.yml')
        raw_data = getattr(loader, 'get_data')(bots_yml)
        if raw_data is None:
            raise ValueError(f'unable to load resource "{bots_yml}"')

        self._bots: list[dict] = YAML(typ='safe').load(raw_data.decode('utf8'))
        self._is_bot = regex.compile('|'.join(bot['regex'] for bot in self._bots))
        self._bots.insert(0, self._bots.pop())  # Move catch-all to front of list.
        self._cache: dict[str, dict[str, object] | bool] = {}

    def inspect(self) -> None:
        """
        Inspect the category metadata. This method iterates over matomo's bot
        definitions and identifies those without category or with the empty
        category. It also prints all unique category values. Since there is
        obvious overlap between several of them, the `BotCategory` label is
        considerably simpler.
        """
        categories: set[str] = set()
        for bot in self._bots:
            name = bot.get('name')
            category = bot.get('category')
            if category is None:
                print('bot without category:', name)
            elif category == '':
                print('bot with empty category:', name)
            else:
                categories.add(category)

        print()
        for category in categories:
            print('category:', category)

    def test(self, user_agent: str) -> bool:
        """Determine whether the user agent is a bot, caching the result."""
        result = self._cache.get(user_agent)
        if result is None:
            result = self._cache[user_agent] = bool(self._is_bot.search(user_agent))
        return bool(result)

    def lookup(self, user_agent: str) -> dict[str, object] | None:
        """Look up the metadata for a bot, caching the result."""
        result = self._cache.get(user_agent)
        if result is None:
            result = self._cache[user_agent] = bool(self._is_bot.search(user_agent))
        if result is True:
            for bot in self._bots:
                pattern = bot.get('pattern')
                if pattern is None:
                    pattern = bot['pattern'] = regex.compile(bot['regex'])
                if pattern.search(user_agent):
                    result = self._cache[user_agent] = bot
                    break
            assert result is not True
        return result if result else None
