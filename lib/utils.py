import yaml

def find_job(job_type, job_name):
    file_path = './job_metadata.yml'

    def _parse_yaml(file_path):
        try:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            return data
        except FileNotFoundError:
            print(f"File not found: {file_path}")

    def _find_job(data, job_name, job_type):
        try:
            for job in data['jobs']:
                if (job['name'] == job_name and job['type'] == job_type):
                    return job
        except KeyError as e:
            print(f'Job not found {e}')

    data = _parse_yaml(file_path)
    return _find_job(data, job_name, job_type)



