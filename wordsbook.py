import argparse
import os
import json
import numpy as np
from pygtrans import Translate
from tqdm import tqdm

def get_parse_data():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
            prog='Wordbook',
            description='generate words for reviewing',
            allow_abbrev=True,
        )
    parser.add_argument('-n', '--num', type=int, help="num_of_words_to_include")
    parser.add_argument('-b', '--begin', type=int, help="fron_which_word", default=1)
    parser.add_argument('-l', '--len', type=int, help="length_of_desired_range")
    parser.add_argument("-r",'--random',action="store_true",help="randomly select the words",)
    parser.add_argument("-f","--file",type=str,help="The_file_including_words_source",default="./collection.txt")
    args = parser.parse_args()

    try:
        assert args.len>=args.num
    except:
        print("You should make sure that there are more words in your desired range than the output")
        exit(1)

    return args.random, args.num, args.begin, args.len, args.file

def figure_out_put_name():
    """检查输出文件夹中已有文件数量，以便为新单词本生成带编号的名称"""
    if not os.path.exists("output"):
        os.mkdir("output")
        os.mkdir("output_translated")
    new_index=len(os.listdir("./output"))+1
    return f"Words {new_index}.txt"

def words_generator(random,n,beginning,length,filename):
    """在第一次使用时生成一个含有所有单词翻译的隐藏json文件；按照命令行参数生成单词本，分别保存到output和output_translated文件夹"""
    with open(filename, "r") as f:
        words_list=f.read().split('\n')
        words_list=[word.strip() for word in words_list if word!='']

    dic_name=".translated.json"
    if os.path.exists(dic_name):
        with open(dic_name) as t:
            dic=json.load(t)
    else:
        print("Initializing the dictionary...\nIt can take a while...")
        client = Translate()
        dic={}
        for each in tqdm(words_list):
            words=each.split(',')
            for word in words:
                try:
                    inChinese=client.translate(word).translatedText
                    dic[word]=inChinese
                except:
                    dic[word]="翻译失败"
        with open(dic_name,'w') as t:
            json.dump(dic,t)
        print("Finished the dictionary.")

    try:
        assert len(words_list)>=length+beginning-1
    except:
        print("There aren't enough words!")
        exit(1)

    output=figure_out_put_name()
    print(f"Generating {output}...")
    out_trans="./output_translated/"+output
    out="./output/"+output
    
    if not random:
        choices=range(n)
    else:
        rng=np.random.default_rng()
        choices=rng.choice(range(length),n,replace=False)

    with open(out,'w') as f:
        with open(out_trans,'w') as ft:
            for i,offset in enumerate(tqdm(choices)):
                ft.write(f"{i+1}: ")
                f.write(f"{i+1}: ")
                index=beginning+offset-1
                item=words_list[index]
                ft.write(f"{item}  :  ")
                f.write(item)
                f.write('\n')
                words=item.split(',')
                for word in words:
                    ft.write(f"{dic[word]}, ")
                ft.write('\n')

    print("Finished!")
       
words_generator(*get_parse_data())