import logging

from config import Config
from reader import FileReader
from dispatcher import Dispatcher
# from result_reporter import Reporter
from worker import Worker


def main():
    config = Config()
    logging.info(f"Start main process with {config.to_json()}")

    dispatcher = Dispatcher(config.system_workers)
    workers = [Worker(i, dispatcher.get_queue(i)) for i in range(config.system_workers)]

    # Start workers
    for worker in workers:
        worker.start()

    logging.info(f"Workers started amount={config.system_workers}")

    # Read file and distribute words
    reader = FileReader(config.file_path, config.windows,dispatcher)
    try:
        logging.info(f"Start filereader from filePath={config.file_path}")
        reader.process_file()
    finally:# Signal workers to stop and wait for completion
        dispatcher.shutdown()

        for worker in workers:
            worker.join()

    # result_reporter = Reporter()
    # result_reporter.generate_report()


if __name__ == "__main__":
    main()
