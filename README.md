# 项目说明：
本项目可使用用户提供的单词表制作带翻译和不带翻译的单词本，并可选择单词范围以及是否随机抽取单词

# 使用：
## 使用前提：
1.可以访问谷歌翻译；2.提前使用pip安装好所需的库

## 大致用法：
可以指定词汇来源文件的路径，要制作的单词本中词汇的数目，待选单词范围以及是否随机选取单词，生成含翻译和不含翻译的单词本；
结果将分别保存在./output和./output_translated路径下；

## 命令行参数：
-r：（可选）随机选取单词
-f：（可选）指定单词来源路径，（不使用该参数则默认在工作目录下寻找名为“collection.txt"的文件作为词汇来源）
-b：（可选）指定待选词汇从词汇来源中的第几个词开始，默认从第一个词开始
-l：指定待选词汇的范围大小
-n：指定要生成的单词本的大小

## 例子：
```python3 wordsbook.py -n 100 -l 200 -r -f "./words.txt"```将以"./words.txt"中的前20个单词为词汇来源，随机选取100个制作单词本

# 注意事项：
1.第一次使用时可能运行时间较长，因为该程序会先对词汇来源中所有单词进行翻译并保存在一个隐藏文件中
2.若需更换词汇来源，请先在工作目录中将名为“.translated.json”的文件删除，否则可能出错
3.新的单词本不会覆盖原有单词本，该程序会自动为单词本编号