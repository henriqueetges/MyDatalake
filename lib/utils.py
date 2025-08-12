import yaml
def parse_yaml(file_path):
    try:
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        return data
    except FileNotFoundError:
        print(f"File not found: {file_path}")


def find_job(job_type, job_name):
    job_dir = job_name.split('_')[0]
    file_path = f'{job_dir}/job_metadata.yml'

    def _find_job(data, job_name, job_type):
        try:
            for job in data['jobs']:
                if (job['name'] == job_name and job['type'] == job_type):
                    return job
        except KeyError as e:
            print(f'Job not found {e}')

    data = parse_yaml(file_path)
    return _find_job(data, job_name, job_type)



