import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

logging.basicConfig()

# Dataset available at
# https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1676561028


class MyExamplePipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input',
            type=str,
            help='Path of the file to read from',
        )
        parser.add_value_provider_argument(
            '--output_increase',
            type=str,
            help='Where to write the increased value props',
        )
        parser.add_value_provider_argument(
            '--output_decrease',
            type=str,
            help='Where to write the decreased value props',
        )
        parser.add_value_provider_argument(
            '--pipeline_name',
            type=str,
            help='This pipe name',
        )


class Split(beam.DoFn):
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def process(self, text):
        yield text.split(self.delimiter)


class PercentageIncreaseCalculator(beam.DoFn):

    def process(self, element):
        new_elem = element[0:10]
        if element[-2] != "" and element[-1] != "":
            new_elem.append((float(element[-1])-float(element[-2]))/float(element[-2]))
            yield new_elem


class Prepare(beam.DoFn):
    def process(self, element):
        yield ",".join([str(e) for e in element])


def execute_pipeline(
        options: PipelineOptions,
        input_location,
        output_location_increase,
        output_location_decrease
):

    with beam.Pipeline(options=options) as pipeline:
        data = pipeline | 'Read File' >> beam.io.ReadFromText(input_location, skip_header_lines=1)
        split = data | 'Split row by comma' >> beam.ParDo(Split())
        percentages = split | 'Perform Action' >> beam.ParDo(PercentageIncreaseCalculator())

        output1 = percentages | 'Filter increased' >> beam.Filter(lambda row: row[-1] > 0.08)
        output1 = output1 | 'Prepare to write increased' >> beam.ParDo(Prepare())
        output1 | 'Write increased' >> beam.io.WriteToText(output_location_increase, file_name_suffix=".csv")

        output2 = percentages | 'Filter decreased' >> beam.Filter(lambda row: row[-1] < -0.02)
        output2 = output2 | 'Prepare to write decreased' >> beam.ParDo(Prepare())
        output2 | 'Write decreased' >> beam.io.WriteToText(output_location_decrease, file_name_suffix=".csv")


def run():
    pipe_options = PipelineOptions().view_as(MyExamplePipelineOptions)
    pipe_options.view_as(SetupOptions).save_main_session = True
    logging.info(f"Pipeline: {pipe_options.pipeline_name}")
    execute_pipeline(
        options=pipe_options,
        input_location=pipe_options.input,
        output_location_increase=pipe_options.output_increase,
        output_location_decrease=pipe_options.output_decrease
    )
    logging.info("FINISHED.")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
