from presentation.CliHandler import CliHandler

path_to_configuration, configuration_number = input(), int(input())
cli_handler = CliHandler(path_to_configuration, configuration_number)
cli_handler.handle()
