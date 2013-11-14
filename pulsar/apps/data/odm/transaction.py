
class ModelDictionary(dict):

    def __contains__(self, model):
        return super(ModelDictionary, self).__contains__(self.meta(model))

    def __getitem__(self, model):
        return super(ModelDictionary, self).__getitem__(self.meta(model))

    def __setitem__(self, model, value):
        super(ModelDictionary, self).__setitem__(self.meta(model), value)

    def get(self, model, default=None):
        return super(ModelDictionary, self).get(self.meta(model), default)

    def pop(self, model, *args):
        return super(ModelDictionary, self).pop(self.meta(model), *args)

    def meta(self, model):
        return getattr(model, '_meta', model)
