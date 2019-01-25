
# coding: utf-8

# ### Поиск повторных событий в массиве событий
# Ключевая функция **convert_to_repeats**

# In[2]:


import pandas as pd, numpy as np, datetime


# In[3]:


from IPython.display import display, clear_output
import time, datetime

def progress_it(func, total_count):
    
    count_current = 0
    
    start_time = time.time()
    prev_time = start_time
    
    def next_step(*args, **kwargs):
        
        nonlocal count_current
        nonlocal prev_time
        count_current += 1
        
        now_time = time.time()
        
        
        if (now_time - prev_time) > 1:
            prev_time = now_time
            progress = count_current/total_count
            
            clear_output(wait=True)
            
            display('Iterations {:.0f} Progress: {:.2%} Completion time: {}'.format(
                count_current, 
                progress,
                datetime.datetime.fromtimestamp(
                    start_time + (now_time - start_time) / progress
                ).strftime('%B %d %H:%M')
            ))
        
        return func(*args, **kwargs)
    
    clear_output(wait=True)
            
    display('Iterations {:.0f} Progress: {:.2%} Completion time: {}'.format(
        0, 
        0,
        '...'
    ))
    
    return next_step


# In[170]:


def get_episode(df, date_column, episode_columns):
    
    data = [df[date_column].min(), None]
    
    for col in episode_columns:
        if type(col) is str:
            data.append(
                '; '.join([
                    str(item)
                    for item in sorted([
                        el for el in list(df[col].unique()) if el not in [None, np.nan, np.NAN, np.NaN, pd.tslib.NaT]
                    ])
                ])
            )
        
        elif type(col) is dict:
            key, func = list(col.items())[0]
            if type(func) is str:
                data.append(df[key].agg(func))
            
            else:
                data.append(func(df[key]))
        
        else:
            data.append(
                '\n'.join([
                    ' | '.join([item for item in row if type(item) is str])
                    for row in df[col].drop_duplicates().sort_values(col[0]).to_dict('split')['data']
                ])
            )
    
    return pd.DataFrame([[data]], columns = ['episode_data'])


# In[169]:


def get_repeats(
    df, 
    leader_columns,
    date_column,
    episode_group_by,
    episode_columns,
    max_episode_length,
    min_episode_length,
    episode_from_start,
    max_days_between
):
    episode_items = sorted(
        [
            item
            for row in df.groupby(episode_group_by).apply(
                get_episode, date_column, episode_columns
            ).to_dict('split')['data']
            for item in row
        ],
        key = lambda x: x[0], 
        reverse = not episode_from_start
    )
    
    max_hours = max_days_between * 24
    
    episodes = []
    episode = []
    
    episode = [episode_items[0]]
    
    for item in episode_items[1:]:
        diff = abs((episode[-1][0] - item[0]).total_seconds()/3600)
        if diff <= max_hours:
            item[1] = diff
            episode.append(item)
        
        else:
            if len(episode) >= min_episode_length: episodes.append(episode[:max_episode_length])
            episode = [item]
    
    if len(episode) >= min_episode_length: episodes.append(episode[:max_episode_length])
    
    data = []
    leaders_data = list(df[leader_columns].iloc[0].values)
    max_columns_count = len(leader_columns) + (len(episode_columns) + 2) * max_episode_length
    
    for episode in episodes:
        row = leaders_data.copy()
        
        for item in episode:
            row.extend(item)
        
        row.extend( [None] * (max_columns_count - len(row)) )
        
        data.append(row)
    
    episode_column_names = [date_column, 'Прошло времени, час.'] + [
        name if type(name) is str else (
            list(name.keys())[0] if type(name) is dict else ' | '.join([str(s) for s in name])
        )
        for name in episode_columns
    ]
    
    names = leader_columns + [ 
        'Эл.{:.0f}: {}'.format(prefix + 1, name)
        for prefix in range(max_episode_length)
        for name in episode_column_names
    ]
    
    return pd.DataFrame(data = data, columns = names)


# In[264]:


def copy_format(book, fmt):
    properties = [f[4:] for f in dir(fmt) if f[0:4] == 'set_']
    dft_fmt = book.add_format()
    return book.add_format({k : v for k, v in fmt.__dict__.iteritems() if k in properties and dft_fmt.__dict__[k] != v})


# In[274]:


def combine_formats(book, *fmts):
    properties = [f[4:] for fmt in fmts for f in dir(fmt) if f[0:4] == 'set_']
    dft_fmt = book.add_format()
    return book.add_format(
        dict([
            (k, v)
            for fmt in fmts
            for k, v in fmt.__dict__.items() if k in properties and dft_fmt.__dict__[k] != v
        ])
    )


# In[277]:


def get_xlsxwiter_formats(df, workbook):
    
    fdatetime = workbook.add_format({'num_format':'dd.mm.yyyy HH:MM', 'align':'left', 'border':4})
    ffloat = workbook.add_format({'num_format':'0.0', 'align':'right', 'border':4})
    fint = workbook.add_format({'num_format':'0', 'align':'right', 'border':4})
    fdef = workbook.add_format()
    
    formats = []
    
    for col in df.columns.tolist():
        t = df.dtypes[col].type
        
        if t in [np.datetime64, datetime.datetime]:
            formats.append(fdatetime)
        
        elif t in [np.float, np.float16, np.float32, np.float64, np.float_, np.float_power]:
            formats.append(ffloat)
        
        elif t in [np.int, np.int0, np.int16, np.int32, np.int64, np.int8, np.int_]:
            formats.append(fint)
        
        else:
            formats.append(fdef)
    
    return formats


# In[292]:


def convert_to_repeats(
    df,
    group_by,
    leader_columns,
    date_column,
    episode_group_by,
    episode_columns,
    max_episode_length,
    min_episode_length = 2,
    episode_from_start = True,
    max_days_between = 32,
    to_excel_file = None
):
    """
    convert_to_repeats(
        df, group_by, leader_columns, date_column, 
        episode_group_by, episode_columns, max_episode_length, 
        min_episode_length = 2, episode_from_start = True, 
        max_days_between = 32, to_excel_file = None
    )
    Объединяет повторяющиеся события в строку. 
    Одна строка - один эпизод. В строке несколько инцидентов. 
    Возможно объединение нескольких инцидентов в один по общему признаку.
    
    Returns: pandas.DataFrame
    
    Аргументы:
    
    df - pandas.DataFrame
    
    group_by - список названий столбцов, по которым производить базовую 
        группировку (например, по клиенту, или по тематике и т.д.):
        group_by = ['ИНН', 'Тема обращения']
    
    leader_columns - список названий столбцов, первые столбцы вывода, 
        они будут выгружены в итоговую таблицу (берется первое 
        значение из каждой группы):
        leader_columns = ['ИНН', 'Наименование организации', 'Тема обращения']
    
    date_column - столбец дат (и времени), по которому определяется 
        является ли одно событие повторным по отношению к другому 
        в заданный промежуток времени.
        date_column = 'Дата и время звонка'
    
    episode_group_by - список названий столбцов, по которым группировать 
        инциденты внутри эпизода (например, по идентификатору звонка).
        episode_group_by = ['ID звонка']
    
    episode_columns - список определений столбцов, которые надо вывести 
        по каждому инциденту:
        episode_columns = [
            
            'Номер обращения', 
            # будут выгружены через ";" список уникальных значений 
            # по группе инцидента. В данном примере - если 
            # по одному 'ID звонка' создалось два обращения, 
            # их номера будут перечислены через точку с запятой.
            
            ['Тема обращения', 'Подтема обращения'], 
            # будут выгружены уникальные сочетания перечисленных столбцов, 
            # значения столбцов разделены знаком "|", сами сочетания знаком ";".
            
            {'Продолжительность', 'max'}, 
            # для указанного в ключе столбца будет вызвана указанная 
            # в значении функция агрегации относительно выборки по инциденту.
            # В данном случае это тождественно 
            # value = df.groupby(...)['Продолжительность'].agg('max')
            # т.е. для столбца 'Продолжительность' будет посчитано максимальное его значение.
            
            {'Продолжительность', func},
            # для указанного в ключе столбца будет вызвана переданная 
            # в значении функция. В функцию будет передан объект 
            # pandas.Series с элементами в группе инцидента.
            # В данном случае это тождественно 
            # value = func( df.groupby(...)['Продолжительность'] )
            
            ... ]
            
    max_episode_length - число, максимальная глубина повторений.
        При этом, важно понимать, если глубина всего эпизода,
        например, 8 повторений, а ограничение max_episode_length = 5,
        то выведено будет 5 элементов, при этом информация об
        оставшихся 3-х элементах будет отброшена.
    
    min_episode_length - число, минимально допустимая глубина повторения,
        если повторений будет меньше - данные будут отброшены.
    
    episode_from_start - True или False, определяет порядок сортировки инцидентов
        внутри эпизода. Если episode_from_start = True, то первым инцидентом в выводе
        будет самый первый инцидент по хронологии, иначе первым инцидентом в выводе
        будет самый последний инцидент в эпизоде, вторым - предпоследний и т.д.
    
    max_days_between - число, максимальный промежуток времени между двумя инцидентами,
        в пределах которого инцидент считается повторным по отношению к предыдущему.
    
    to_excel_file - текст, наименование файла с расширением .xlsx, 
        в который автоматически будет экспортирован вывод с форматированием.
        Если to_excel_file = None (по умолчанию) - то экспорт будет проигнорирован.
        to_excel_file = 'Some Excel filename.xlsx'
    
    Важно:
    
    Будут удалены все строки, содержащие пустые значения хотя бы в одной 
    ячейке столбцов group_by и episode_group_by.
    
    """
    g = df.dropna(subset = group_by + episode_group_by).groupby(group_by, as_index = False)
    
    g_df = g.apply(
        progress_it(get_repeats, len(g)), 
        leader_columns, 
        date_column, 
        episode_group_by, 
        episode_columns, 
        max_episode_length, 
        min_episode_length,
        episode_from_start,
        max_days_between
    )
    
    if to_excel_file is not None:
        print('Сохранение в файл {}...'.format(to_excel_file))

        sheet_name = 'Лист1'
        writer = pd.ExcelWriter(to_excel_file, engine = 'xlsxwriter')
        
        book  = writer.book
        sheet = book.add_worksheet(sheet_name)
        
        formats = get_xlsxwiter_formats(g_df, book)
        fheaders = [book.add_format({'bold':True, 'align':'center', 'valign':'vcenter', 'text_wrap':True, 'border':1})] * len(formats)
        fevencol = book.add_format({'bg_color':'silver'})
        
        first_col_idx = len(leader_columns)
        episode_columns_count = (len(g_df.columns) - first_col_idx) / max_episode_length
        for col_idx_range in range(0, max_episode_length, 2):
            for col_idx in range(
                int(first_col_idx + col_idx_range*episode_columns_count), 
                int(first_col_idx + col_idx_range*episode_columns_count + episode_columns_count)
            ):
                formats[col_idx] = combine_formats(book, formats[col_idx], fevencol)
                fheaders[col_idx] = combine_formats(book, fheaders[col_idx], fevencol)
        
        
        for col_index, value in enumerate([
            col if type(col) is str else '. '.join(list(col))
            for col in g_df.columns.tolist()
        ]):
            sheet.write(0, col_index, value, fheaders[col_index])
        
        for row_index, data in enumerate(g_df.itertuples(False), 1):
            for col_index, value in enumerate(data):
                if (value != value) or (value == np.inf): value = None
                sheet.write(row_index, col_index, value, formats[col_index])

        writer.save()

        print('Сохранение завершено.')
        
    return g_df

