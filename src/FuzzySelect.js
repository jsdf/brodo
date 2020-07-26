import React from 'react';
import ReactTags from 'react-tag-autocomplete';
import Fuse from 'fuse.js';
import './react-tags.css';

const {useState, useCallback, useEffect, useRef, useMemo} = React;

// get array of unique values with comparison based on mapping function, where
// first occurrence is kept, and subsequent duplicates are removed
function uniqueBy(arr, mapper) {
  const seen = new Set();

  return arr.filter((v) => {
    const key = mapper(v);

    if (seen.has(key)) {
      return false;
    } else {
      seen.add(key);
      return true;
    }
  });
}

// get array a with values from b subtracted, with comparison based on mapping function
function subtractBy(a, b, mapper) {
  const bKeys = new Set(b.map(mapper));
  return a.filter((aVal) => !bKeys.has(mapper(aVal)));
}

function search(options, filter) {
  var fuseOptions = {
    shouldSort: true,
    tokenize: true,
    threshold: 0.6,
    location: 0,
    distance: 100,
    maxPatternLength: 32,
    minMatchCharLength: 0,
    keys: ['name'],
  };
  var fuse = new Fuse(options, fuseOptions);
  return fuse.search(filter);
}

function FuzzySelect({options, value, onChange}) {
  const tags = value;
  const [suggestions, setSuggestions] = useState([]);

  const reactTagsRef = useRef(null);

  const onDelete = useCallback(
    (i) => {
      const nextTags = tags.slice(0);
      nextTags.splice(i, 1);
      onChange(nextTags);
    },
    [tags]
  );

  const onAddition = useCallback(
    (tag) => {
      const nextTags = uniqueBy([].concat(tags, tag), (v) => v.id);
      onChange(nextTags);
    },
    [tags]
  );

  const candidateOptions = useMemo(() => {
    return subtractBy(options, tags, (tag) => tag.id);
  }, [options, tags]);

  const onInput = useCallback(
    (input) => {
      const newSuggestions = search(candidateOptions, input).map(
        (res) => res.item
      );

      setSuggestions(newSuggestions);
    },
    [candidateOptions]
  );

  const onKeyDown = useCallback(
    (e) => {
      // when one of the terminating keys is pressed, add current query to the tags
      if (['Tab', 'Enter'].indexOf(e.key) > -1) {
        e.preventDefault();
        if (suggestions.length) {
          onAddition(suggestions[0]);
        }
        if (reactTagsRef.current) {
          reactTagsRef.current.clearInput();
        }
      }
    },
    [suggestions]
  );

  return (
    <span onKeyDown={onKeyDown}>
      <ReactTags
        ref={reactTagsRef}
        tags={tags}
        suggestions={suggestions}
        minQueryLength={1}
        onDelete={onDelete}
        onAddition={onAddition}
        onInput={onInput}
      />
    </span>
  );
}

export default FuzzySelect;
