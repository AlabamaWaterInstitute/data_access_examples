from defs import xr_read_window, polymask, xr_read_window_time
from rasterio import _io, windows
import xarray as xr
import pandas as pd


class MemoryDataset(_io.MemoryDataset, windows.WindowMethodsMixin):
    pass


def junk():

    flist = gpkg_divides.geometry.to_list()
    polys = flist


def get_forcing_dict_newway(
    feature_index,
    feature_list,
    folder_prefix,
    file_list,
    var_list,
):

    reng = "rasterio"

    _xds_dummy = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds_dummy.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds_dummy.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )

    ds_list = []
    for _nc_file in file_list:
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        ds_list.append(xr.open_dataset(_full_nc_file, engine=reng))

    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=feature_index)

    for i, feature in enumerate(feature_list):
        print(f"{i}, {round(i/len(feature_list), 5)*100}".ljust(40), end="\r")
        mask, _, window = polymask(_u2d)(feature)
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        for j, _xds in enumerate(ds_list):
            time_value = _xds.time.values[0]
            cropped = xr_read_window(_xds, winslices, mask=mask)
            stats = cropped.mean()
            for var in var_list:
                df_dict[var].loc[i, time_value] = stats[var]

    [ds.close() for ds in ds_list]
    return df_dict


def get_forcing_dict_newway_parallel(
    feature_index,
    feature_list,
    folder_prefix,
    file_list,
    var_list,
    para="thread",
    para_n=2,
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
    ds_list = [xr.open_dataset(folder_prefix.joinpath(f)) for f in file_list]
    # ds_list = [xr.open_dataset(folder_prefix.joinpath(f), engine=reng) for f in file_list]
    # TODO: figure out why using the rasterio engine DOES NOT WORK with parallel
    # TODO: figure out why NOT using the rasterio engine produces a different result

    if para == "process":
        pool = concurrent.futures.ProcessPoolExecutor
    elif para == "thread":
        pool = concurrent.futures.ThreadPoolExecutor
    else:
        pool = concurrent.futures.ThreadPoolExecutor

    stats = []
    future_list = []

    with pool(max_workers=para_n) as executor:

        for _i, _m in enumerate(map(polymask(_u2d), feature_list)):
            print(f"{_i}, {round(_i/len(feature_list), 5)*100}".ljust(40), end="\r")
            mask, _, window = _m
            mask = xr.DataArray(mask, dims=["y", "x"])
            winslices = dict(zip(["y", "x"], window.toslices()))
            for ds in ds_list:
                _t = ds.time.values[0]
                future = executor.submit(
                    xr_read_window_time, ds, winslices, mask=mask, idx=_i, time=_t
                )
                # cropped = xr_read_window(f, winslices, mask=mask)
                # stats.append(cropped.mean())
                future_list.append(future)
        for _f in concurrent.futures.as_completed(future_list):
            _j, _t, _s = _f.result()
            stats.append((_j, _t, _s))

    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=feature_index)

    for j, t, s in stats:
        for var in var_list:
            df_dict[var].loc[j, t] = s[var].mean()

    [ds.close() for ds in ds_list]
    return df_dict


def get_forcing_dict_newway_inverted(
    feature_index,
    feature_list,
    folder_prefix,
    file_list,
    var_list,
):

    reng = "rasterio"

    _xds_dummy = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds_dummy.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds_dummy.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )
    ds_list = []
    for _nc_file in file_list:
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        ds_list.append(xr.open_dataset(_full_nc_file, engine=reng))

    stats = []
    mask_win_list = []

    for i, feature in enumerate(feature_list):
        print(f"{i}, {round(i/len(feature_list), 5)*100}".ljust(40), end="\r")
        mask, _, window = polymask(_u2d)(feature)
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        mask_win_list.append((mask, winslices))

    for i, f in enumerate(ds_list):
        print(f"{i}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
        time_value = f.time.values[0]
        # TODO: when we read the window, could the time be added as a dimension?
        for j, (_m, _w) in enumerate(mask_win_list):
            cropped = xr_read_window(f, _w, mask=_m)
            stats.append((j, time_value, cropped.mean()))

    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=feature_index)

    for j, t, s in stats:
        for var in var_list:
            df_dict[var].loc[j, t] = s[var]

    [ds.close() for ds in ds_list]
    return df_dict


def get_forcing_dict_newway_inverted_parallel(
    feature_index,
    feature_list,
    folder_prefix,
    file_list,
    var_list,
    para="thread",
    para_n=2,
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

    ds_list = [xr.open_dataset(folder_prefix.joinpath(f)) for f in file_list]
    # ds_list = [xr.open_dataset(folder_prefix.joinpath(f), engine=reng) for f in file_list]
    # TODO: figure out why using the rasterio engine DOES NOT WORK with parallel
    # TODO: figure out why NOT using the rasterio engine produces a different result

    stats = []
    future_list = []

    if para == "process":
        pool = concurrent.futures.ProcessPoolExecutor
    elif para == "thread":
        pool = concurrent.futures.ThreadPoolExecutor
    else:
        pool = concurrent.futures.ThreadPoolExecutor

    with pool(max_workers=para_n) as executor:

        for j, ds in enumerate(ds_list):
            print(f"{j}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
            _t = ds.time.values[0]
            for _i, (_m, _w) in enumerate(mask_win_list):
                future = executor.submit(
                    xr_read_window_time, ds, _w, mask=_m, idx=_i, time=_t
                )
                # cropped = xr_read_window(ds, _w, mask=_m)
                # stats.append(cropped.mean())
                future_list.append(future)
        for _f in concurrent.futures.as_completed(future_list):
            _j, _t, _s = _f.result()
            stats.append((_j, _t, _s))

    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=feature_index)

    for j, t, s in stats:
        for var in var_list:
            df_dict[var].loc[j, t] = s[var].mean()

    [ds.close() for ds in ds_list]
    return df_dict
