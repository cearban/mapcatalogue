import unittest
from cataloger import reverse_geocode_wgs84_boundingbox


class TestGeocoder(unittest.TestCase):
    """
        unittests for the geocoder function
    """
    def setUp(self):
        self.pg_conn_str = "postgres://james:MopMetal3@localhost:5432/mapcatalogue"
        self.us_bbox = (-74.66163264559283, 39.650417182507944, -72.0006154054558, 41.612140592278074)

    def test_geocoder_returns_list(self):
        self.assertIsInstance(
            reverse_geocode_wgs84_boundingbox(self.pg_conn_str, self.us_bbox),
            list,
            'Should be a list'
        )

    def test_geocoder_contains_dict(self):
        self.assertIsInstance(
            reverse_geocode_wgs84_boundingbox(self.pg_conn_str, self.us_bbox)[0],
            dict,
            'List should contain dictionaries'
        )

    def test_geocoder_equals(self):
        bboxes = {
            1: [
                (-84.7, 28.5, -66.8, 42.6),
                [{'country': 'Canada', 'continent': 'North America'},
                 {'country': 'United States', 'continent': 'North America'}]
            ],
            2: [
                (-4.080, 55.572, -2.228, 57.250),
                [{'country': 'England', 'continent': 'Europe'},
                 {'country': 'Scotland', 'continent': 'Europe'}]
            ],
        }

        for bb in bboxes:
            bbox_coords = bboxes[bb][0]
            geographies = bboxes[bb][1]

            self.assertEqual(
                reverse_geocode_wgs84_boundingbox(self.pg_conn_str, bboxes[bb][0]),
                bboxes[bb][1],
                'Geographies for BBox should equal: {0}'.format(bboxes[bb][1])
            )


if __name__ == "__main__":
    unittest.main()

