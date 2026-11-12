"""Gold boundaries, references, leakage contracts, Parquet read-back and HDFS publish."""
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing import gold
from processing.silver import Hdfs, create_spark


class GoldChecks(unittest.TestCase):
    def test_cleaning_contract_and_paths(self):
        self.assertEqual(gold.clean_text('<p>Not &amp; never 10 mm</p>'), 'Not & never 10 mm')
        self.assertEqual(gold.clean_text('Voltage <10 V; USB-C'), 'Voltage <10 V; USB-C')
        self.assertEqual(gold.review_text('Good', 'Good'), 'Good')
        self.assertEqual(gold.item_text('A', ['<p>A</p>', 'Not bad', 'Not bad', '']), 'A\nNot bad')
        self.assertEqual(gold.item_text(None, ['Useful description']), 'Useful description')
        self.assertEqual(gold.item_text('Title', None), 'Title')
        self.assertEqual(gold.text_id(''), None)
        self.assertEqual(gold.text_id(gold.clean_text('a  b')), gold.text_id('a b'))
        self.assertEqual(gold.read_contract()[0]['status'], 'locked')
        for path in ('hdfs://namenode:8020/project/raw', 'hdfs://namenode:8020/project/processed/gold/../silver', 'file:///tmp/gold'):
            with self.assertRaises(ValueError):
                gold.validate_location(path, '/project/processed/gold', False, 'output')

    def test_folds_and_export(self):
        spark = create_spark(Path('/tmp/gold-fixture-unit'), 2, 2)
        try:
            rows = [
                ('r0','u','c',datetime(2019,1,1)),
                ('r1','u','a',datetime(2020,1,1)), ('r2','u','a',datetime(2020,2,1)),
                ('r3','v','b',datetime(2020,1,1)),
                ('r4','u','a',datetime(2021,1,1)), ('r5','u','z',datetime(2021,1,1)),
                ('r6','u','b',datetime(2021,4,1)), ('r7','cold','b',datetime(2021,2,1)),
                ('r8','boundary','a',datetime(2021,7,1)), ('r9','u','a',datetime(2022,1,1)),
            ]
            events=spark.createDataFrame(rows,'record_hash string,user_id string,parent_asin string,event_time timestamp')
            tables={}
            def emit(name,frame,keys):
                tables[name]=[r.asDict() for r in frame.collect()]
            counts=gold.build_fold(events,'All_Beauty','validation','2021-01-01','2021-07-01',2,emit,lambda _:None)
            self.assertEqual((counts['train_pairs'],counts['queries'],counts['warm_full_queries']),(3,2,0))
            self.assertEqual(len(tables['labels']),3)
            self.assertEqual({r['parent_asin'] for r in tables['labels']},{'a','z','b'})
            anchors={(r['user_id'],r['parent_asin']):r for r in tables['train_examples']}
            self.assertFalse(anchors['u','a']['history_empty'])
            self.assertEqual(anchors['u','a']['prediction_time'],datetime(2020,1,1))
            self.assertTrue(anchors['u','c']['history_empty'])
            self.assertEqual({r['user_id']:r['user_idx'] for r in tables['users']},{'u':0,'v':1})
            self.assertTrue(any(r['item_idx'] is None for r in tables['labels']))
            self.assertFalse(any(k in tables['queries'][0] for k in ('target_time','target_record_hash','rating','review_text')))
            self.assertTrue(any(r['unknown_user'] for r in tables['queries']))
            reviews=events.withColumn('review_title',gold.F.lit('Not bad')).withColumn('review_text',gold.F.lit('<p>Keep 10 mm</p>'))
            catalog=spark.createDataFrame([('a','item-a','Title A',['Title A','Useful description','Useful description']),('b','item-b',None,[])],'parent_asin string,record_hash string,product_title string,description array<string>')
            assessment=gold.description_profile(catalog)
            self.assertEqual(assessment['with_description'],1)
            self.assertEqual(assessment['description_adds_text'],1)
            candidates=catalog.select('parent_asin')
            with tempfile.TemporaryDirectory() as directory:
                package=Path(directory)/'texts'
                output=gold.export_text(reviews,catalog,'2022-01-01',candidates,emit,package,{'category':'All_Beauty'},lambda _:None,2,2**30)
                self.assertEqual(output['item_references'],2)
                self.assertEqual(output['item_blank'],1)
                self.assertEqual(output['review_references'],9)
                self.assertEqual(output['texts'],2)  # unique title and one shared review text
                self.assertTrue(all(f['rows']<=2 for f in output['files']))
                self.assertEqual({r['record_hash'] for r in tables['review_text_refs']},{f'r{i}' for i in range(9)})
                for path in (package/'texts').glob('*.parquet'):
                    self.assertNotIn('user_id',spark.read.parquet(path.as_uri()).columns)
        finally:
            spark.stop()

    @unittest.skipUnless(os.environ.get('GOLD_TEST_HDFS')=='1','HDFS fixture opt-in')
    def test_atomic_publish_and_failed_run(self):
        stamp=datetime.now().strftime('%Y%m%d%H%M%S%f')
        source=f'hdfs://namenode:8020/project/testing/processed/gold/source_{stamp}'
        output=f'hdfs://namenode:8020/project/testing/processed/gold/output_{stamp}'
        spark=create_spark(Path('/tmp/gold-fixture-'+stamp),2,2)
        try:
            reviews=spark.createDataFrame([
                ('r1','u','a',datetime(2020,1,1),'Title','Not bad'),
                ('r2','u','b',datetime(2021,1,1),'Title','Not bad'),
                ('r3','cold','a',datetime(2022,1,1),'New','Future review'),
            ],'record_hash string,user_id string,parent_asin string,event_time timestamp,review_title string,review_text string')
            catalog=spark.createDataFrame([('a','item-a','A',['Details A']),('b','item-b','B',[])],'parent_asin string,record_hash string,product_title string,description array<string>')
            reviews.write.parquet(source+'/reviews_clean');catalog.write.parquet(source+'/catalog')
            Hdfs(spark).write_json(source+'/manifest.json',dict(status='complete',category='All_Beauty',schema=dict(reviews=reviews.schema.jsonValue(),catalog=catalog.schema.jsonValue())))
        finally:
            spark.stop()
        args=dict(category='All_Beauty',testing=True,fixture_source=source,output_base=output,shuffle_partitions=2,min_free_gib=0)
        report=Path('/workspace/outputs/processing/gold')/('run_success_'+stamp)
        report.mkdir(parents=True)
        (report/'batch.json').write_text('{"status":"fixture_host_created_directory"}')
        result=gold.run_gold(**args,run_id='run_success_'+stamp)
        self.assertEqual(result['status'],'complete')
        self.assertEqual(result['folds']['validation']['queries'],1)
        self.assertEqual(result['text_export']['review_references'],2)
        with self.assertRaisesRegex(FileExistsError,'already exists'):
            gold.run_gold(**args,run_id='run_success_'+stamp)
        with self.assertRaisesRegex(RuntimeError,'Injected failure'):
            gold.run_gold(**args,run_id='run_failure_'+stamp,fail_before_publish=True)
        spark=create_spark(Path('/tmp/gold-check-'+stamp),2,2)
        try:
            hdfs=Hdfs(spark)
            self.assertTrue(hdfs.exists(result['final_path']+'/manifest.json'))
            self.assertFalse(hdfs.exists(f'{output}/all_beauty/v1/run_failure_{stamp}'))
        finally:
            spark.stop()


if __name__=='__main__':
    unittest.main()
