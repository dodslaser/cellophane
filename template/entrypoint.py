from runpy import run_path

if __name__ == "__main__":
    run_path(".", run_name="__main__", init_globals={"__file__": "foo"})
