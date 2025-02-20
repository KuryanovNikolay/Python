"""
Simplified VM code which works for some cases.
You need extend/rewrite code to pass all cases.
"""

import builtins
import dis
import types
import typing as tp
import operator


class Frame:
    """
    Frame header in cpython with description
        https://github.com/python/cpython/blob/3.12/Include/internal/pycore_frame.h

    Text description of frame parameters
        https://docs.python.org/3/library/inspect.html?highlight=frame#types-and-members
    """

    def __init__(self,
                 frame_code: types.CodeType,
                 frame_builtins: dict[str, tp.Any],
                 frame_globals: dict[str, tp.Any],
                 frame_locals: dict[str, tp.Any]) -> None:
        self.code = frame_code
        self.builtins = frame_builtins
        self.globals = frame_globals
        self.locals = frame_locals
        self.data_stack: tp.Any = []
        self.return_value = None
        self.output_buffer: list[tp.Any] = []

    def top(self) -> tp.Any:
        return self.data_stack[-1]

    def pop(self, i: int = 0) -> tp.Any:
        return self.data_stack.pop(-1 - i)

    def push(self, *values: tp.Any) -> None:
        self.data_stack.extend(values)

    def popn(self, n: int) -> tp.Any:
        """
        Pop a number of values from the value stack.
        A list of n values is returned, the deepest value first.
        """
        if n > 0:
            returned = self.data_stack[-n:]
            self.data_stack[-n:] = []
            return returned
        else:
            return []

    def run(self) -> tp.Any:
        for instruction in dis.get_instructions(self.code):
            getattr(self, instruction.opname.lower() + "_op")(instruction.argval)
        return self.return_value

    def resume_op(self, arg: int) -> tp.Any:
        pass

    def push_null_op(self, arg: int) -> tp.Any:
        self.push(None)

    def precall_op(self, arg: int) -> tp.Any:
        pass

    def call_op(self, arg: int) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-CALL
        """
        if self.data_stack and isinstance(self.data_stack[-1], tuple):
            keyword_names = self.pop() if arg != 1 else ()
            keyword_values = [self.pop() for _ in range(len(keyword_names))] if keyword_names else []
            kwargs = dict(zip(keyword_names, reversed(keyword_values))) if keyword_names else {}
        else:
            keyword_names = ()
            kwargs = {}

        num_positional_args = arg - len(kwargs)

        positional_args = [self.pop() for _ in range(num_positional_args)]
        positional_args.reverse()

        function = self.pop()
        function = function if callable(function) else self.pop()

        try:
            result = function(*positional_args, **kwargs)
        except Exception as error:
            raise error

        self.push(result)
        self.keyword_names = None

    def load_name_op(self, arg: str) -> None:
        """
        Partial realization

        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-LOAD_NAME
        """
        if arg in self.locals:
            self.push(self.locals[arg])
        elif arg in self.globals:
            self.push(self.globals[arg])
        elif arg in builtins.__dict__:
            self.push(builtins.__dict__[arg])
        else:
            raise NameError(f"name '{arg}' is not defined'")

    def store_global_op(self, arg: str) -> None:
        self.globals[arg] = self.pop()

    def load_global_op(self, arg: str) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-LOAD_GLOBAL
        """
        if arg in self.globals:
            self.push(self.globals[arg])
        elif arg in builtins.__dict__:
            self.push(builtins.__dict__[arg])
        else:
            raise NameError(f"Global name {arg} not found")

    def load_const_op(self, arg: tp.Any) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-LOAD_CONST
        """
        self.push(arg)

    def return_const_op(self, arg: tp.Any) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-RETURN_VALUE
        """
        self.return_value = arg
        self.output_buffer.append(str(arg))

    def pop_top_op(self, arg: tp.Any) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-POP_TOP
        """
        self.pop()

    def make_function_op(self, arg: int) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-MAKE_FUNCTION
        """
        code = self.pop()  # the code associated with the function (at TOS1)

        def f(*args: tp.Any, **kwargs: tp.Any) -> tp.Any:

            parsed_args: dict[str, tp.Any] = {}
            f_locals = dict(self.locals)
            f_locals.update(parsed_args)

            frame = Frame(code, self.builtins, self.globals, f_locals)  # Run code in prepared environment
            return frame.run()

        self.push(f)

    def store_name_op(self, arg: str) -> None:
        """
        Operation description:
            https://docs.python.org/release/3.12.5/library/dis.html#opcode-STORE_NAME
        """
        const = self.pop()
        self.locals[arg] = const

    def binary_op_op(self, arg: int) -> None:
        second = self.pop()
        first = self.pop()

        operations = {
            0: lambda x, y: x + y,
            1: lambda x, y: x & y,
            2: lambda x, y: x // y,
            3: lambda x, y: x << y,
            5: lambda x, y: x * y,
            6: lambda x, y: x % y,
            7: lambda x, y: x | y,
            8: lambda x, y: x ** y,
            9: lambda x, y: x >> y,
            10: lambda x, y: x - y,
            11: lambda x, y: x / y,
            12: lambda x, y: x ^ y,
            4: lambda x, y: x @ y,
            13: lambda x, y: str(x) + str(y) if isinstance(x, str) or isinstance(y, str) else x + y,
            14: lambda x, y: x & y,
            15: lambda x, y: x // y,
            16: lambda x, y: x << y,
            18: lambda x, y: x * y,
            19: lambda x, y: x % y,
            20: lambda x, y: x | y,
            21: lambda x, y: x ** y,
            22: lambda x, y: x >> y,
            23: lambda x, y: x - y,
            24: lambda x, y: x / y,
            25: lambda x, y: x ^ y,
            17: lambda x, y: x @ y,
        }

        if arg in operations:
            self.push(operations[arg](first, second))
        else:
            raise ValueError(f"Unsupported binary operation with index {arg}")

    def compare_op_op(self, arg: str) -> None:
        second = self.pop()
        first = self.pop()

        comparison_operators = {
            '<': operator.lt,
            '<=': operator.le,
            '==': operator.eq,
            '!=': operator.ne,
            '>': operator.gt,
            '>=': operator.ge,
        }
        comparison_operator = comparison_operators[arg]
        self.push(comparison_operator(first, second))

    def contains_op_op(self, arg: int) -> None:
        container, element = self.pop(), self.pop()
        self.push(arg == 0 and element in container or arg == 1 and element not in container)

    def print_op(self, arg: int) -> None:
        data = [str(self.pop()) for _ in range(arg)]
        self.output_buffer.append(' '.join(reversed(data)) + '\n')  # Сохраняем результат в буфер
        self.push(None)

    def unary_op(self, arg: int) -> None:
        data = self.pop()
        if arg == 12:
            self.push(not data)
        elif arg == 11:
            self.push(-data)
        elif arg == 15:
            self.push(~data)
        else:
            raise ValueError(f"Unsupported unary operation with index {arg}")

    def build_tuple_op(self, arg: int) -> None:
        elements = self.popn(arg)
        new_tuple = tuple(elements)
        self.push(new_tuple)

    def build_list_op(self, arg: int) -> None:
        elements = self.popn(arg)
        new_list = list(elements)
        self.push(new_list)


class VirtualMachine:
    def run(self, code_obj: types.CodeType) -> None:
        """
        :param code_obj: code for interpreting
        """
        globals_context: dict[str, tp.Any] = {}
        frame = Frame(code_obj, builtins.globals()['__builtins__'], globals_context, globals_context)
        return frame.run()
