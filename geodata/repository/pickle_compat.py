import io
import pickle


_CLASS_REMAP = {
    ('datainterface.DemographicProfile', 'DemographicProfile'): (
        'geodata.datainterface.DemographicProfile',
        'DemographicProfile',
    ),
    ('datainterface.GeoVector', 'GeoVector'): (
        'geodata.datainterface.GeoVector',
        'GeoVector',
    ),
}


class _CompatUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        remapped = _CLASS_REMAP.get((module, name))
        if remapped:
            module, name = remapped
        return super().find_class(module, name)


def compat_load(file_obj):
    return _CompatUnpickler(file_obj).load()


def compat_loads(data):
    return _CompatUnpickler(io.BytesIO(data)).load()
