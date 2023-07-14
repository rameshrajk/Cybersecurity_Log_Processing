def preprocess(grouped, cols=['process_owner', 'process_name', 'parent_process_name', 'computername', 'parent_executablepath', 'executable_path', 'hour'], elastic_ids=False, generate=False):
    if elastic_ids:
        grouped['elastic_id'] = grouped['elastic_id'].apply(lambda x: x.strip('][').split(', '))

    grouped['process_owner'] = grouped['process_owner'].parallel_apply(lambda x: re.sub('\d+', '*', x.lower()))
    grouped['computername'] = grouped['computername'].parallel_apply(lambda x: re.sub('\d+', '*', x))

    grouped['executable_path'] = grouped['executable_path'].parallel_apply(lambda x: '\\'.join(x.split('\\')[:-1]))
    if 'parent_executablepath' in cols:
        grouped['parent_executablepath'] = grouped['executable_path'].parallel_apply(lambda x: '\\'.join(x.split('\\')[:-1]))

        grouped['parent_executablepath'] = grouped['parent_executablepath'].parallel_apply(lambda x: re.sub('[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}', 'UUID', x.lower()))
        grouped['parent_executablepath'] = grouped['parent_executablepath'].parallel_apply(lambda x: re.sub('0[xX][0-9a-fA-F]+', 'HEX', x.lower()))

    grouped['executable_path'] = grouped['executable_path'].parallel_apply(lambda x: re.sub('[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}', 'UUID', x.lower()))
    grouped['executable_path'] = grouped['executable_path'].parallel_apply(lambda x: re.sub('0[xX][0-9a-fA-F]+', 'HEX', x.lower()))

    grouped['process_name'] = grouped['process_name'].parallel_apply(lambda x: re.sub('[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}', 'UUID', x.lower()))
    grouped['process_name'] = grouped['process_name'].parallel_apply(lambda x: re.sub('0[xX][0-9a-fA-F]+', 'HEX', x.lower()))

    grouped['parent_process_name'] = grouped['parent_process_name'].parallel_apply(lambda x: re.sub('[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}', 'UUID', x.lower()))
    grouped['parent_process_name'] = grouped['parent_process_name'].parallel_apply(lambda x: re.sub('0[xX][0-9a-fA-F]+', 'HEX', x.lower()))

    if 'parent_executablepath' in cols:
        grouped['parent_executablepath'] = grouped['parent_executablepath'].parallel_apply(lambda x: re.sub('\d+', '*', x.lower()))
    grouped['executable_path'] = grouped['executable_path'].parallel_apply(lambda x: re.sub('\d+', '*', x.lower()))
    grouped['process_name'] = grouped['process_name'].parallel_apply(lambda x: re.sub('\d+', '*', x.lower()))
    grouped['parent_process_name'] = grouped['parent_process_name'].parallel_apply(lambda x: re.sub('\d+', '*', x.lower()))

    if not generate:
        if elastic_ids:
            grouped = grouped.groupby(cols, as_index=False).agg({'count': sum, 'elastic_id': list, 'min_time': min, 'max_time': max})
            grouped['elastic_id'] = grouped['elastic_id'].apply(lambda x: [item for sublist in x for item in sublist])
        else:
            grouped = grouped.groupby(cols, as_index=False).agg({'count': sum, 'min_time': min, 'max_time': max})

    return grouped
