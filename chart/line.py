from pyecharts.charts import Line
import pyecharts.options as opts
from example.commons import Faker

data = [176, 186, 280, 252, 288, 306, 362, 373, 387, 437, 467, 433, 456, 652, 612, 552, 550, 635, 553]
x_data = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012',
          '2013', '2014', '2015', '2016', '2017', '2018']


def line_base() -> Line:
    c = (
        Line()
            .add_xaxis(xaxis_data=x_data)
            .add_yaxis("", data)
            .set_global_opts(title_opts=opts.TitleOpts(title="每年图书数量", pos_left='center'),
                             toolbox_opts=opts.ToolboxOpts(is_show=True))
        # (opts.TooltipOpts(is_show=True))
    )
    return c


line_demo = line_base()
line_demo.render('./html/line.html')
