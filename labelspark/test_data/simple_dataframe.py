import unittest
import pandas as pd

class MyTestCase(unittest.TestCase):
    def test_something(self):
        simple_dataframe = {"external_id": ["image1.jpg", "image2.jpg"],
                            "row_data": ["https://commons.wikimedia.org/wiki/File:Cute_grey_kitten.jpg",
                                         "https://en.wikipedia.org/wiki/File:Red_Kitten_01.jpg"],
                            "metadata_field_1": [123, 456],
                            "metadata_field_2": ["Cat looking over there", "Cat looking over here"]}

        test_df = pd.DataFrame.from_dict(simple_dataframe)

        print(test_df)

        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
