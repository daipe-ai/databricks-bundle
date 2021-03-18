from inspect import signature as create_inspect_signature
from injecta.service.class_.InspectedArgumentResolver import InspectedArgumentResolver


def inspect_function(obj):
    inspected_argument_resolver = InspectedArgumentResolver()
    signature = create_inspect_signature(obj)

    return list(map(lambda argument: inspected_argument_resolver.resolve(argument[0], argument[1]), signature.parameters.items()))
