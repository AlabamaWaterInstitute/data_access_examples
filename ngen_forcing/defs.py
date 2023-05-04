import rasterio.mask as riomask

def polymask(dataset, invert=False, all_touched=False):
    def _polymask(poly):
        return riomask.raster_geometry_mask(
            dataset, [poly], invert=invert, all_touched=all_touched, crop=True
        )

    return _polymask


def xr_read_window(ds, window, mask=None):
    data = ds.isel(window)
    if mask is None:
        return data
    else:
        return data.where(mask)


def xr_read_window_time(ds, window, mask=None, idx=None, time=None):
    data = ds.isel(window)
    if mask is None:
        return idx, time, data
    else:
        return idx, time, data.where(mask)