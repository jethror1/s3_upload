import argparse


def parse_args() -> argparse.Namespace:
    """
    Parse cmd line arguments

    Returns
    -------
    argparse.Namespace
        parsed arguments
    """
    parser = argparse.ArgumentParser()

    # TODO - add the args
    # need to decide on what running modes to have and user config

    return parser.parse_args()


def main() -> None:
    args = parse_args()


if __name__ == "__main__":
    main()
