from defs import xr_read_window, polymask
from rasterio import _io, windows
import concurrent.futures
import xarray as xr


class MemoryDataset(_io.MemoryDataset, windows.WindowMethodsMixin):
    pass


def junk():

    flist = gpkg_divides.geometry.to_list()
    polys = flist


def get_forcing_dict_newway(
    feature_list,
    folder_prefix,
    file_list,
):

    reng = "rasterio"
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )

    filehandles = [xr.open_dataset("data/" + f) for f in file_list]
    stats = []
    for i, m in enumerate(map(polymask(_u2d), feature_list)):
        print(f"{i}, {round(i/len(feature_list), 5)*100}".ljust(40), end="\r")
        mask, _, window = m
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        for f in filehandles:
            cropped = xr_read_window(f, winslices, mask=mask)
            stats.append(cropped.mean())
    [f.close() for f in filehandles]  # Returns None for each file

    return stats


def get_forcing_dict_newway_parallel(
        feature_list,
        folder_prefix,
        file_list,
        ):

    reng = "rasterio"
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values

    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )
    filehandles = [xr.open_dataset("data/" + f) for f in file_list]

    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
    # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        stats = []
        future_list = []

        for i, m in enumerate(map(polymask(_u2d), feature_list)):
            print(f"{i}, {round(i/len(feature_list), 5)*100}".ljust(40), end="\r")
            mask, _, window = m
            mask = xr.DataArray(mask, dims=["y", "x"])
            winslices = dict(zip(["y", "x"], window.toslices()))
            for f in filehandles:
                future = executor.submit(xr_read_window, f, winslices, mask=mask)
                # cropped = xr_read_window(f, winslices, mask=mask)
                # stats.append(cropped.mean())
                future_list.append(future)
        for _f in concurrent.futures.as_completed(future_list):
            stats.append(_f.result().mean())

    [f.close() for f in filehandles]
    return stats


def get_forcing_dict_newway_inverted(
    feature_list,
    folder_prefix,
    file_list,
):

    reng = "rasterio"
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )

    filehandles = [xr.open_dataset("data/" + f) for f in file_list]
    stats = []
    future_list = []
    mask_win_list = []

    for i, m in enumerate(map(polymask(_u2d), feature_list)):
        print(f"{i}, {round(i/len(feature_list), 5)*100}".ljust(40), end="\r")
        mask, _, window = m
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        mask_win_list.append((mask, winslices))

    for f in filehandles:
        print(f"{i}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
        for _m, _w in mask_win_list:
            cropped = xr_read_window(f, _w, mask=_m)
            stats.append(cropped.mean())

    [f.close() for f in filehandles]
    return stats


def get_forcing_dict_newway_inverted_parallel(
        feature_list,
        folder_prefix,
        file_list,
        ):

    import concurrent.futures

    reng = "rasterio"
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )

    mask_win_list = []
    for i, m in enumerate(map(polymask(_u2d), feature_list)):
        print(f"{i}, {round(i/len(feature_list), 5)*100}".ljust(40), end="\r")
        mask, _, window = m
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        mask_win_list.append((mask, winslices))

    filehandles = [xr.open_dataset("data/" + f) for f in file_list]
    stats = []
    future_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:

        for f in filehandles:
            print(f"{i}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
            for _m, _w in mask_win_list:
                future = executor.submit(xr_read_window, f, _w, mask=_m)
                # cropped = xr_read_window(f, _w, mask=_m)
                # stats.append(cropped.mean())
                future_list.append(future)
        for _f in concurrent.futures.as_completed(future_list):
            stats.append(_f.result().mean())

    [f.close() for f in filehandles]
    return stats
