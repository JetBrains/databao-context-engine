from databao_context_engine.plugins.plugin_loader import load_plugins


def main():
    plugins = load_plugins()
    plugin_ids = {plugin.id for plugin in plugins.values()}
    print(repr(plugin_ids))


if __name__ == "__main__":
    main()
