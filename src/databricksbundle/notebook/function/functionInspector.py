from inspect import signature as createInspectSignature
from injecta.service.class_.InspectedArgumentResolver import InspectedArgumentResolver

def inspectFunction(obj):
    inspectedArgumentResolver = InspectedArgumentResolver()
    signature = createInspectSignature(obj)

    return list(map(lambda argument: inspectedArgumentResolver.resolve(argument[0], argument[1]), signature.parameters.items()))
