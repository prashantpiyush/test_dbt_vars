import shlex
from dbt.cli.main import dbtRunner, dbtRunnerResult

runner = dbtRunner()


def execute_dbt_cmd(command):
    print("Running dbt command ", command)
    response = runner.invoke(command.split()[1:])
    # response = runner.invoke(shlex.split(command)[1:])
    print("command and response", command, str(response))
    if response.success:
        return response.result
    elif response.exception:
        raise response.exception
    else:
        results = getattr(response.result, 'results', [getattr(response.result, 'result', None)])
        results = [x for x in results if x is not None]
        for result in results:
            if str(result.status) == 'error':
                raise Exception(result.message)
        return results


props = " --vars {\"EDW_DATABASE\":\"PPIYUSH_TEST\"}"
cmd = f'dbt run --model sf_model --profile snowflake {props}'
print(shlex.split(cmd)[1:])
print(cmd.split()[1:])
rs = execute_dbt_cmd(cmd)
print(rs)
