from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import pipeline

if __name__ == '__main__':
    options = PipelineOptions()
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DirectRunner'

    input_location = "/Users/maccheroni/Downloads/Neighborhood_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    # output_location = "gs://alex_migrator_test/themis-test/output/data"
    output_location = "/Users/maccheroni/Downloads"

    pipeline.execute_pipeline(options, input_location, output_location)
