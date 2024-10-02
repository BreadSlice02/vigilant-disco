import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly

import argparse
import datetime
import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)


class JobOptions(PipelineOptions):
    """
    These are command line options for CLI usage.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", required=True, help="Pub/Sub input subscription")


def parse_message(message):
    """
    Parses the message into a dictionary.
    """
    record = message.decode('utf-8').split(';')
    return {
        'user_id': record[0],
        'fullname': record[1],
        'url': record[2],
        'timestamp': record[3],
        'bytes': int(record[4])
    }


def run(argv=None, save_main_session=True):
    """
    Run function to process CLI args and run your program.
    """
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.append('--allow_unsafe_triggers')

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    job_options = pipeline_options.view_as(JobOptions)
    pipeline_options.view_as(StandardOptions).streaming = True

    logging.info("-----------------------------------------------------------")
    logging.info(" Streaming with Pub/Sub emulator ")
    logging.info("-----------------------------------------------------------")

    source = ReadFromPubSub(subscription=str(job_options.input))

    p = beam.Pipeline(options=pipeline_options)

    lines = (
        p
        | "Read from PubSub" >> source
        | "Parse Messages" >> beam.Map(parse_message)
        | "Window into Fixed Intervals" >> beam.WindowInto(
            window.FixedWindows(60),
            trigger=Repeatedly(AfterProcessingTime(60)),
            allowed_lateness=30,
            accumulation_mode=AccumulationMode.ACCUMULATING
        )
        | "Map to Key-Value" >> beam.Map(lambda x: ((x['user_id'], x['url']), x['bytes']))
        | "Sum Bytes" >> beam.CombinePerKey(sum)
        | "Format Results" >> beam.Map(lambda x: f"User: {x[0][0]}, URL: {x[0][1]}, Total Bytes: {x[1]}")
        | "Print Results" >> beam.Map(print)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()
