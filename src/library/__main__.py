import library.service.lib as lib
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("starting lib")
    lib.main()


if __name__ == "__main__":
    main()
